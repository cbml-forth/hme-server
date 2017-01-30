/*
 * Copyright 2016-2017 FORTH-ICS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gr.forth.ics.cbml.chic.hme.server;

import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.Db;
import com.github.pgasync.Row;
import gr.forth.ics.cbml.chic.hme.server.modelrepo.ModelRepository;
import gr.forth.ics.cbml.chic.hme.server.modelrepo.SemanticStore;
import gr.forth.ics.cbml.chic.hme.server.utils.FileUtils;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.io.IoCallback;
import io.undertow.security.api.SecurityContext;
import io.undertow.security.idm.Account;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.PredicateHandler;
import io.undertow.server.handlers.resource.*;
import io.undertow.server.session.InMemorySessionManager;
import io.undertow.server.session.SessionAttachmentHandler;
import io.undertow.server.session.SessionCookieConfig;
import io.undertow.util.*;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.aeonbits.owner.ConfigFactory;
import org.pac4j.core.config.Config;
import org.pac4j.saml.client.SAML2Client;
import org.pac4j.saml.metadata.SAML2MetadataResolver;
import org.pac4j.undertow.account.Pac4jAccount;
import org.pac4j.undertow.context.UndertowWebContext;
import org.pac4j.undertow.handler.ApplicationLogoutHandler;
import org.pac4j.undertow.handler.CallbackHandler;
import org.pac4j.undertow.handler.SecurityHandler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.undertow.Handlers.routing;

public class HmeServer {


    private final static ExecutorService executorService = Executors.newFixedThreadPool(10);

    private static void quickly_dispatch(final HttpServerExchange exchange, Runnable func) {
        exchange.dispatch(exchange.isInIoThread() ? SameThreadExecutor.INSTANCE : exchange.getIoThread(),
                func);
    }

    private static Path previewsDir;

    private static Db initDatabase() {


        // Use UTC as default timezone when retrieving timestamps from the DB
        System.setProperty("user.timezone", "UTC");

        Db db = new ConnectionPoolBuilder()
                .hostname(config.dbHost())
                .port(5432)
                .database(config.dbName())
                .username(config.dbUser())
                .password(config.dbPwd())
                .validationQuery("SET TIMEZONE='UTC'") // Use UTC as the connection timezone
                .poolSize(20)
                .build();

        return db;
    }

    static int svgToImage(Path inputSvgFile, Path outputSvgPath, int x, int y, int width, int height,
                          int q) {

        String styles = "svg{background:white}" +
                ".connection-wrap{display:none}" +
                ".marker-vertices{display:none}" +
                ".link-tools{display:none}" +
                ".marker-arrowheads{display:none}" +
                "path.connection{fill:none}" +
                ".port-label {display:none}" +
                "text.label{display:none}";

        if (x < 0) {
            width += x;
            x = 0;
        }
        if (y < 0) {
            height += y;
            y = 0;
        }

        String quality = String.format("%d%%", 100); //XXX

        final String viewbox = String.format("%d:%d:%d:%d", x, y, width, height);
        ProcessBuilder processBuilder = new ProcessBuilder("svgexport",
                inputSvgFile.toString(),
                outputSvgPath.toString(),
                quality,
                viewbox,
                // "5000:5000", "crop",
                styles);
        processBuilder.inheritIO();

        // System.err.println(processBuilder.command().stream().collect(Collectors.joining(" ")));

        try {
            Process p = processBuilder.start();
            int exitCode = p.waitFor();
            if (exitCode == 0 && q < 100) {
                // Optimize??
                int maxSizeKB = 20;

                ProcessBuilder processBuilder2 = new ProcessBuilder("jpegoptim",
                        "--size=" + maxSizeKB,
                        outputSvgPath.toString());
                processBuilder2.inheritIO();
                Process p2 = processBuilder2.start();
                exitCode = p2.waitFor();
            }
            return exitCode;
        } catch (InterruptedException | IOException e) {
            e.printStackTrace(System.err);
            return -1;
        }
    }

    static void transcode(final HttpServerExchange exchange) {

        if (exchange.isInIoThread()) {
            exchange.dispatch(HmeServer::transcode);
            return;
        }

        exchange.startBlocking();
        try {

            Path tempDirectory = Files.createTempDirectory("");

            Path inputSvgFile = tempDirectory.resolve("input.svg");
            Path outputSvgPath = tempDirectory.resolve("output.jpg");

            Files.copy(exchange.getInputStream(), inputSvgFile);

            final Map<String, Deque<String>> queryParameters = exchange.getQueryParameters();
            int x = 0, y = 0;
            int width = 2000, height = 2000;
            if (queryParameters.containsKey("x"))
                x = Integer.parseInt(queryParameters.get("x").getFirst());
            if (queryParameters.containsKey("y"))
                y = Integer.parseInt(queryParameters.get("y").getFirst());
            if (queryParameters.containsKey("w"))
                width = Integer.parseInt(queryParameters.get("w").getFirst());
            if (queryParameters.containsKey("h"))
                height = Integer.parseInt(queryParameters.get("h").getFirst());

            int quality = 100; // 0 - 100
            int exitCode = svgToImage(inputSvgFile, outputSvgPath, x, y, width, height, quality);

            if (exitCode == 0) {
                FileResourceManager fileResourceManager = new FileResourceManager(outputSvgPath.getParent().toFile(),
                        1024 * 1024);
                Resource resource = fileResourceManager.getResource(outputSvgPath.getFileName().toString());
                resource.serve(exchange.getResponseSender(), exchange, IoCallback.END_EXCHANGE);
            } else {
                exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
                exchange.getResponseSender().send("svgexport returned " + exitCode, IoCallback.END_EXCHANGE);
            }

        } catch (Exception e) {
            e.printStackTrace();
            sendException(exchange, e);

        }
    }

    static void previewSvg(final HttpServerExchange exchange, final Path inputSvgFile, final Path outputSvgPath, final ETag etag) {
        try {

            final Map<String, Deque<String>> queryParameters = exchange.getQueryParameters();

            int x = 0, y = 0;
            int width = 2000, height = 2000;
            if (queryParameters.containsKey("x"))
                x = Integer.parseInt(queryParameters.get("x").getFirst());
            if (queryParameters.containsKey("y"))
                y = Integer.parseInt(queryParameters.get("y").getFirst());
            if (queryParameters.containsKey("w"))
                width = Integer.parseInt(queryParameters.get("w").getFirst());
            if (queryParameters.containsKey("h"))
                height = Integer.parseInt(queryParameters.get("h").getFirst());

            int quality = 99; // 0 - 100
            if (queryParameters.containsKey("q"))
                quality = Integer.parseInt(queryParameters.get("q").getFirst());

            int exitCode = svgToImage(inputSvgFile, outputSvgPath, x, y, width, height, quality);


            if (exitCode == 0) {
                FileResourceManager fileResourceManager = new FileResourceManager(outputSvgPath.getParent().toFile(),
                        1024 * 1024);
                Resource resource = fileResourceManager.getResource(outputSvgPath.getFileName().toString());
                exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "image/jpeg");
                exchange.getResponseHeaders().add(Headers.ETAG, etag.toString());
                exchange.getResponseHeaders().add(Headers.CACHE_CONTROL, "max-age:36500");
                exchange.getResponseHeaders().add(Headers.CONTENT_LENGTH, outputSvgPath.toFile().length());
                resource.serve(exchange.getResponseSender(), exchange, IoCallback.END_EXCHANGE);
            } else {
                exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
                exchange.getResponseSender().send("svgexport returned " + exitCode, IoCallback.END_EXCHANGE);
            }

        } catch (Exception e) {

            sendException(exchange, e);

        }
    }

    static void serveStaticFile(ResourceManager res_mgr, HttpServerExchange exchange) {
        String requestPath = exchange.getRequestPath();
        String path = CanonicalPathUtils.canonicalize(requestPath.replace("/static", ""));
        System.out.printf("[] get_static_file %s\n", path);
        try {
            final Resource resource = res_mgr.getResource(path);
            if (resource != null)
                resource.serve(exchange.getResponseSender(), exchange, IoCallback.END_EXCHANGE);
            else {
                exchange.setStatusCode(StatusCodes.NOT_FOUND);
                exchange.getResponseSender().send(path + " was not found", IoCallback.END_EXCHANGE);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Db init() throws IOException {
        previewsDir = Paths.get(System.getProperty("java.io.tmpdir"), "hme-previews");
        System.err.println("Preview (temp) dir: " + previewsDir);
        if (!previewsDir.toFile().exists())
            Files.createDirectories(previewsDir);

        final Db database = initDatabase();
        return database;

    }


    private static Pac4jAccount getAccount(final HttpServerExchange exchange) {
        final SecurityContext securityContext = exchange.getSecurityContext();
        if (securityContext != null) {
            final Account account = securityContext.getAuthenticatedAccount();
            if (account instanceof Pac4jAccount) {
                return (Pac4jAccount) account;
            }
        }
        return null;
    }

    private static HmeServerConfig config;

    private static void apiError401(final HttpServerExchange exchange) {
        final String msg = "Authentication error: You need to login, please visit " +
                BASE_PATH + "/login?url=" + exchange.getRequestPath();
        sendError(exchange, msg, StatusCodes.UNAUTHORIZED);

    }

    private static void redirect(final HttpServerExchange exchange, String uri)
    {
        exchange.setStatusCode(StatusCodes.FOUND);
        exchange.getResponseHeaders().put(Headers.LOCATION, uri);
        exchange.endExchange();
    }

    private static HttpHandler createLoginHandler(final String contParam, final String homeUrl) {
        return exchange -> {
            final Map<String, Deque<String>> queryParameters = exchange.getQueryParameters();
            final String nextUrl = queryParameters.containsKey(contParam) ?
                    queryParameters.get(contParam).getFirst()
                    : homeUrl;
            redirect(exchange, nextUrl);
        };
    }

    private static HttpHandler createLoginRedirectHandler(final String contParam, final String loginUrl) {
        return exchange -> {
            final String requestPath = exchange.getRequestPath();
            final String nextUrl = loginUrl +"?" + contParam + "=" + requestPath;
            redirect(exchange, nextUrl);
        };
    }

    private static String BASE_PATH = "/hme2";

    public static void main(String[] args) throws IOException {

        final HmeServerConfig serverConfig = ConfigFactory.create(HmeServerConfig.class);

        HmeServer.config = serverConfig;

        int port = serverConfig.port();

        final Db database = init();


        final ModelRepository modelRepository = new ModelRepository(10, true);
        final SAMLLogin samlLogin = new SAMLLogin(config.secureTokenService());
        final String endpoint = config.sparqlRicordo();
        System.err.println("RICORDO : " + endpoint);
        final SemanticStore semanticStore = new SemanticStore(endpoint);

        final Config samlConfig = new SamlConfigFactory("https://ssfak.duckdns.org/hme2/").build();

        final RoutingHandler apiRoutes = routing(true)
                .get("/preview/{uuid}/{version}",
                        exchange -> exchange.dispatch(() -> preview_hypermodel(exchange, database)))
                .get("/models", exchange -> exchange.dispatch(() -> getAllModels(exchange, samlLogin, database, modelRepository, semanticStore)))

                .get("/hypermodels", exchange -> exchange.dispatch(() -> get_hypermodels(exchange, database)))
                .post("/hypermodels", exchange -> exchange.dispatch(() -> save_hypermodel(exchange, database)))
                .get("/hypermodels/{uuid}/{version}",
                        exchange -> exchange.dispatch(() -> get_hypermodel(exchange, database)))
                .get("/hypermodels/{uuid}", exchange -> exchange.dispatch(() -> get_hypermodel(exchange, database)))
                .get("/transcode", HmeServer::transcode);

        final HttpHandler corsHandler = new CORSHandler().wrap(apiRoutes);

        final String editorUrl = BASE_PATH + "/static/html/";
        final HttpHandler loginHandler = createLoginHandler("url", editorUrl);
        final HttpHandler redirectToLoginHandler = createLoginRedirectHandler("url", BASE_PATH + "/login");
        final ResourceHandler resourceHandler = new ResourceHandler(
                new PathResourceManager(Paths.get("/Users/ssfak/Documents/Projects/CHIC/WP10/editor/chic-hme-ng/src"), 1024))
                .setWelcomeFiles("index.html");
        HttpHandler rootHandler = Handlers.path()
                // Redirect root path to /static to serve the index.html by default
                .addExactPath("/", new PredicateHandler(ChicAccount.authnPredicate(),
                        Handlers.redirect(editorUrl), redirectToLoginHandler))

                .addPrefixPath("/h", new PredicateHandler(ChicAccount.authnPredicate(),
                        routing(true).get("{uuid}",
                                exchange -> redirect(exchange, editorUrl +"#" + exchange.getQueryParameters().get("uuid").getFirst())),
                                redirectToLoginHandler))

                // Serve all static files from a folder
                .addPrefixPath("/static", new PredicateHandler(ChicAccount.authnPredicate(),
                        resourceHandler, redirectToLoginHandler))

                // REST API path
                .addPrefixPath("/api", new PredicateHandler(ChicAccount.authnPredicate(), corsHandler, HmeServer::apiError401))

                .addExactPath("/login", SecurityHandler.build(loginHandler, samlConfig, "SAML2Client"))

                // Security related endpoints:
                .addExactPath("/metadata", exchange -> HmeServer.samlMetadata(exchange, samlConfig))
                .addExactPath("/callback", CallbackHandler.build(samlConfig, null, true))
                .addExactPath("/logout", new ApplicationLogoutHandler(samlConfig, BASE_PATH + "/"));


        final SessionCookieConfig sessionConfig = new SessionCookieConfig();
        sessionConfig.setCookieName("HMESESSIONID");
        sessionConfig.setPath(FileUtils.endWithSlash(BASE_PATH));
        final SessionAttachmentHandler sessionManager =
                new SessionAttachmentHandler(Handlers.path().addPrefixPath(BASE_PATH, rootHandler),
                        new InMemorySessionManager("SessionManager"),
                        sessionConfig);
        Undertow server = Undertow.builder()
                .addHttpListener(port, HmeServer.config.hostname())
                .setWorkerThreads(20)
                .setHandler(sessionManager).build();
//                .setHandler(corsHandler).build();
        System.err.println("All ready.. Start listening on " + HmeServer.config.hostname() + ":" + port);
        server.start();
    }

    private static void samlMetadata(final HttpServerExchange exchange, Config samlConfig) {

        final SAML2Client client = (SAML2Client) samlConfig.getClients().findAllClients().get(0);

        if (client.getServiceProviderMetadataResolver() == null) {
            UndertowWebContext context = new UndertowWebContext(exchange);

            client.init(context);
        }
        final SAML2MetadataResolver serviceProviderMetadataResolver = client.getServiceProviderMetadataResolver();
        final String metadata = serviceProviderMetadataResolver.getMetadata();

        exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/xml");
        exchange.getResponseSender().send(metadata, IoCallback.END_EXCHANGE);
    }

    private static CompletableFuture<List<Map<String, String>>> getAnnotationsOfModels(SemanticStore semanticStore) {

        final String q = "PREFIX chic:  <http://www.chic-vph.eu/ontologies/resource#>\n" +
                "SELECT ?uuid (GROUP_CONCAT(distinct ?p1) AS ?perspective1) \n" +
                "             (GROUP_CONCAT(distinct ?p2) AS ?perspective2) \n" +
                "             (GROUP_CONCAT(distinct ?p3) AS ?perspective3) \n" +
                "             (GROUP_CONCAT(distinct ?p4) AS ?perspective4) \n" +
                "             (GROUP_CONCAT(distinct ?p5) AS ?perspective5) \n" +
                "             (GROUP_CONCAT(distinct ?p6) AS ?perspective6) \n" +
                "             (GROUP_CONCAT(distinct ?p7) AS ?perspective7) \n" +
                "             (GROUP_CONCAT(distinct ?p8) AS ?perspective8) \n" +
                "             (GROUP_CONCAT(distinct ?p9) AS ?perspective9) \n" +
                "             (GROUP_CONCAT(distinct ?p10) AS ?perspective10) \n" +
                "             (GROUP_CONCAT(distinct ?p11) AS ?perspective11) \n" +
                "             (GROUP_CONCAT(distinct ?p12) AS ?perspective12) \n" +
                "             (GROUP_CONCAT(distinct ?p13) AS ?perspective13)\n" +
                "WHERE {\n" +
                "  ?x a  chic:Model-ChicHypomodel; chic:hasCHICuuid ?uuid;\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-1  ?p1  .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-2  ?p2  .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-3  ?p3  .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-4  ?p4  .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-5  ?p5  .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-6  ?p6  .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-7  ?p7  .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-8  ?p8  .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-9  ?p9  .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-10 ?p10 .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-11 ?p11 .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-12 ?p12 .}\n" +
                "  OPTIONAL {?x  chic:hasPositionIn-13 ?p13 .}\n" +
                "}\n" +
                "GROUP BY ?uuid";

        // System.err.println(""+q);
        return semanticStore.send_query_and_parse(q);
    }

    private static void getAllModels(HttpServerExchange exchange, SAMLLogin login, Db db, ModelRepository modelRepository, SemanticStore semanticStore) {
        System.err.println("-> getAllModels");

        ChicAccount.currentUser(exchange).map(Object::toString).ifPresent(System.out::println);
        ChicAccount.currentUser(exchange).map(ChicAccount::attrsToString).ifPresent(System.out::println);
        Optional<String> actAs = ChicAccount.currentUser(exchange).map(_user -> "crafsrv");
        final CompletableFuture<List<Map<String, String>>> annotationsOfModels = getAnnotationsOfModels(semanticStore);
        final CompletableFuture<Map<String, Integer>> mapCompletableFuture = countHypermodelsPerModel(db);
        login.createToken(config.serviceAccountName(), config.serviceAccountPassword(), modelRepository.AUDIENCE, actAs)
                .thenCompose(modelRepository::getAllModels)
                .thenCombine(mapCompletableFuture, (models, counts) -> models.stream().map(model -> {
                    final JSONObject json = model.toJSON();
                    json.put("usage", counts.getOrDefault(model.getUuid(), 0));
                    return json;
                }).collect(Collectors.toList()))
                .thenCombine(annotationsOfModels, (jsonList, annotations) -> {
                    System.err.println("Got models and annotations");
                    final Map<String, List<Map<String, String>>> annsPerModel = annotations.stream().collect(Collectors.groupingBy(m -> m.get("uuid")));
                    JSONArray a = new JSONArray();
                    jsonList.forEach(json -> {
                        final String uuid = json.getAsString("uuid");
                        if (annsPerModel.containsKey(uuid)) {
                            final Map<String, String> m = annsPerModel.get(uuid).get(0);
                            m.remove("uuid");
                            final JSONObject persp = new JSONObject();
                            m.forEach((k, v) -> {
                                if (!"".equals(v)) {
                                    final String[] strings = v.split(" ");
                                    final JSONArray jsonArray = new JSONArray();
                                    jsonArray.addAll(Arrays.asList(strings));
                                    persp.put(k, jsonArray);
                                }
                            });
                            json.put("perspectives", persp);
                        } else
                            json.put("perspectives", new JSONObject());
                        a.add(json);
                    });

                    return a;
                })
                .handle((jsonArray, throwable) -> {

                    if (throwable != null)
                        sendException(exchange, throwable);
                    else {
                        exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
                        exchange.getResponseSender().send(jsonArray.toJSONString(), IoCallback.END_EXCHANGE);
                    }

                    return null;
                });

    }


    private static String userId(final HttpServerExchange exchange) {
        return "stelios"; // XXX
    }

    private static void get_hypermodels(final HttpServerExchange exchange, Db db) {
        final String user_id = userId(exchange);

        final String sql =
                "SELECT * FROM hypermodel_versions_vw WHERE user_id=$1 AND hypermodel_version=most_recent_version";

        db.query(sql, Arrays.asList(user_id),
                rs -> {
                    JSONParser p = new JSONParser(JSONParser.MODE_RFC4627);
                    JSONArray l = new JSONArray();
                    rs.forEach(row -> {
                        try {
                            JSONObject js = rowToHypermodel(row, p);
                            l.add(js);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    });
                    exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send(l.toJSONString(), IoCallback.END_EXCHANGE);

                },
                throwable -> sendException(exchange, throwable));
    }

    private static void get_hypermodel(final HttpServerExchange exchange, Db db) {
        final String user_id = userId(exchange);
        final Map<String, Deque<String>> queryParameters = exchange.getQueryParameters();
        final String param = queryParameters.get("uuid").getFirst();
        final Long aVersion = queryParameters.containsKey("version") ?
                Long.valueOf(queryParameters.get("version").getFirst()) : null;

        final UUID uuid;
        try {
            uuid = UUID.fromString(param);
        } catch (IllegalArgumentException ex) {
            sendError(exchange,
                    "Hypermodel '" + param + "' was not found!",
                    StatusCodes.NOT_FOUND);
            return;
        }

        final String sql = "SELECT * FROM hypermodel_versions_vw" +
                " WHERE user_id=$1 AND hypermodel_uid=$2 AND hypermodel_version=" +
                (aVersion == null ? "most_recent_version" : "$3");

        final List params = aVersion == null ? Arrays.asList(user_id, uuid) : Arrays.asList(user_id, uuid, aVersion);
        db.query(sql, params,
                rs -> {
                    if (rs.size() == 0) {
                        sendError(exchange,
                                "Hypermodel '" + uuid + "' (version=" + aVersion + ") was not found!",
                                StatusCodes.NOT_FOUND);
                        return;

                    }
                    final Row row = rs.row(0);

                    final Long version = row.getLong("hypermodel_version");
                    final Timestamp last_update = row.getTimestamp("version_created");
//                    final Date gmtDate = Date.from(last_update.toInstant(ZoneOffset.UTC));
                    String etag = String.format("%d", version);
                    final ETag eTag = new ETag(false, etag);


                    final HeaderMap responseHeaders = exchange.getResponseHeaders();
                    responseHeaders.add(Headers.ETAG, eTag.toString());
                    if (ETagUtils.handleIfNoneMatch(exchange, eTag, eTag.isWeak())) {
                        try {
                            JSONObject js = rowToHypermodel(row);
                            // Allow caching for 2 hours:
                            // (https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control)
                            responseHeaders.add(Headers.CACHE_CONTROL, "max-age:7200, private, must-revalidate");
                            responseHeaders.add(Headers.CONTENT_TYPE, "application/json");
                            responseHeaders.add(Headers.LAST_MODIFIED, DateUtils.toDateString(last_update));
                            exchange.getResponseSender().send(js.toJSONString(), IoCallback.END_EXCHANGE);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            sendException(exchange, e);
                            return;
                        }
                    } else {
                        exchange.setStatusCode(StatusCodes.NOT_MODIFIED);
                        exchange.endExchange();
                    }


                },
                throwable -> sendException(exchange, throwable));
    }


    private static Path previewPathFor(final String hypermodel_uid, long version) {

        final String outputImgFilename = hypermodel_uid + "-" + version + ".jpg";
        Path outputImgFile = previewsDir.resolve(outputImgFilename);
        return outputImgFile;
    }

    private static String getPathETag(final Path path) {
        final File file = path.toFile();
        if (!file.exists())
            return "";
        final String s = path.getFileName() + ":" + file.lastModified() + ":" + file.length();
        return s;
    }

    static void preview_hypermodel(final HttpServerExchange exchange, Db db) {
        final String user_id = userId(exchange);

        final Map<String, Deque<String>> queryParameters = exchange.getQueryParameters();
        final String param = queryParameters.get("uuid").getFirst();
        final Long version = Long.valueOf(queryParameters.get("version").getFirst());

        final UUID uuid;
        try {
            uuid = UUID.fromString(param);
        } catch (IllegalArgumentException ex) {
            exchange.setStatusCode(StatusCodes.NOT_FOUND);
            exchange.getResponseSender().send("Hypermodel '" + param + "' was not found!", IoCallback.END_EXCHANGE);
            return;
        }
//        String etag = queryParameters.entrySet().stream()
//                .map(entry -> entry.getKey() + ":" + entry.getValue().getFirst())
//                .collect(Collectors.joining("/"));

        Path outputImgFile = previewPathFor(param, version);
        String etag = getPathETag(outputImgFile);
        final ETag eTag = new ETag(false, etag);

        if (!ETagUtils.handleIfNoneMatch(exchange, eTag, eTag.isWeak())) {
            exchange.setStatusCode(StatusCodes.NOT_MODIFIED);
            exchange.getResponseHeaders().add(Headers.CACHE_CONTROL, "max-age=43200");
            exchange.getResponseHeaders().add(Headers.ETAG, eTag.toString());

            exchange.endExchange();
            return;
        }

        if (outputImgFile.toFile().exists()) {
            FileResourceManager fileResourceManager = new FileResourceManager(previewsDir.toFile(),
                    1024 * 1024);
            Resource resource = fileResourceManager.getResource(outputImgFile.getFileName().toString());
            exchange.getResponseHeaders().add(Headers.CACHE_CONTROL, "max-age=43200");
            exchange.getResponseHeaders().add(Headers.ETAG, eTag.toString());
            exchange.getResponseHeaders().add(Headers.CONTENT_LENGTH, outputImgFile.toFile().length());
            resource.serve(exchange.getResponseSender(), exchange, IoCallback.END_EXCHANGE);
            return;

        }

        final String sql = "SELECT svg_content FROM hypermodels" +
                " JOIN hypermodel_versions USING (hypermodel_uid)" +
                " WHERE user_id=$1 AND hypermodel_uid=$2 AND hypermodel_version=$3";

        db.query(sql, Arrays.asList(user_id, uuid, version),
                rs -> {
                    if (rs.size() == 0) {
                        sendError(exchange,
                                "Hypermodel '" + uuid + "' (version=" + version + ") was not found!",
                                StatusCodes.NOT_FOUND);
                        return;

                    }


                    final Row row = rs.row(0);
                    final String svgContent = row.getString("svg_content");
                    final String svgContentNoHtmlEntities = svgContent.replace("&nbsp;", " ");


                    try {
                        Path inputSvgFile = Files.createTempFile("", ".svg");
                        Files.write(inputSvgFile, svgContentNoHtmlEntities.getBytes(StandardCharsets.UTF_8));
                        executorService.submit(() -> previewSvg(exchange, inputSvgFile, outputImgFile, eTag));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                },
                throwable -> sendException(exchange, throwable));
    }

    static void sendException(final HttpServerExchange exchange, final Throwable throwable) {
        sendException(exchange, throwable, StatusCodes.INTERNAL_SERVER_ERROR);
    }

    static void sendException(final HttpServerExchange exchange, final Throwable throwable, int code) {
        throwable.printStackTrace();
        sendError(exchange, throwable.getMessage(), code);
    }

    static void sendError(final HttpServerExchange exchange, final String msg, int code) {
        /*
        // Send an Error response according to https://tools.ietf.org/html/rfc7807
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("status", code);
        jsonObject.put("title", msg);
        final String contentType = "application/problem+json";
        */

        // Send an error according to the JSON Google guide
        // (https://google.github.io/styleguide/jsoncstyleguide.xml)
        final JSONObject error = new JSONObject();
        error.put("code", code);
        error.put("message", msg);
        final String contentType = "application/json";
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("error", error);

        exchange.setStatusCode(code);
        exchange.getResponseHeaders().clear();
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, contentType);
        exchange.getResponseSender().send(jsonObject.toJSONString(), IoCallback.END_EXCHANGE);
    }

    private static CompletableFuture<Map<String, Integer>> countHypermodelsPerModel(final Db db) {

        CompletableFuture<Map<String, Integer>> fut = new CompletableFuture<>();
        db.query("select model_uuid, count(*)::int4" +
                        " FROM hypermodel_versions_models JOIN recent_versions_vw USING (hypermodel_uid)" +
                        " WHERE hypermodel_version = most_recent_version GROUP BY model_uuid"
                , rs -> {
                    final HashMap<String, Integer> map = new HashMap<>();
                    for (Row r : rs) {
                        final String model_uuid = r.getString(0);
                        final Integer cnt = r.getInt(1);
                        map.put(model_uuid, cnt);

                    }
                    fut.complete(map);
                }
                , fut::completeExceptionally);
        return fut;
    }

    private static ETag hypermodelEtag(final Long version) {
        return hypermodelEtag(version == null ? -1 : version);
    }

    private static ETag hypermodelEtag(final long version) {
        final boolean weakEtag = false;
        final String etag = version < 0 ? "-" : String.format("%d", version);
        return new ETag(weakEtag, etag);
    }

    private static void save_hypermodel(final HttpServerExchange exchange, Db db) {

        final String user_id = userId(exchange);
        exchange.startBlocking();
        try {
            JSONParser parser = new JSONParser(JSONParser.MODE_RFC4627);
            final JSONObject o = (JSONObject) parser.parse(exchange.getInputStream());
            final String uuid = o.getAsString("uuid");
            final String title = o.getAsString("title");
            final String description = o.getAsString("description");
            final String canvas = o.getAsString("canvas");
            final String svgContent = o.getOrDefault("svg_content", "").toString();
            final JSONObject graph = (JSONObject) o.get("graph");

            // System.err.println("GOT : " + o.toJSONString());

            final boolean frozen = (Boolean) o.getOrDefault("frozen", Boolean.FALSE);

            db.query("WITH upd AS " +
                            "(INSERT INTO hypermodels(hypermodel_uid,user_id)" +
                            " VALUES($1,$2) ON CONFLICT (hypermodel_uid) DO UPDATE SET updated=now()" +
                            " RETURNING *)" +
                            " SELECT MAX(hypermodel_version) most_recent_version" +
                            " FROM hypermodel_versions JOIN upd USING (hypermodel_uid)",
                    Arrays.asList(uuid, user_id),
                    rs -> {

                        final Long most_recent_version = rs.row(0).getLong(0);

                        final ETag eTag = hypermodelEtag(most_recent_version);

                        if (rs.size() == 0 || ETagUtils.handleIfMatch(exchange, eTag, eTag.isWeak())) {
                            db.query(
                                    "INSERT INTO hypermodel_versions(hypermodel_uid, title, description, svg_content, json_content, graph_content, frozen)" +
                                            " VALUES($1, $2, $3, $4, $5, $6, $7)" +
                                            " RETURNING hypermodel_version",
                                    Arrays.asList(uuid, title, description, svgContent, canvas, graph.toJSONString(), frozen),
                                    rs1 -> {
                                        final Long version = rs1.row(0).getLong(0);
                                        final ETag eTag2 = hypermodelEtag(version);

                                        exchange.setStatusCode(StatusCodes.CREATED);
                                        exchange.getResponseHeaders()
                                                .add(Headers.LOCATION, String.format("/hypermodels/%s/%d", uuid, version))
                                                .add(Headers.ETAG, eTag2.toString());
                                        exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
                                        final JSONObject obj = new JSONObject(Collections.singletonMap("version",
                                                "" + version));
                                        exchange.getResponseSender().send(obj.toJSONString(), IoCallback.END_EXCHANGE);


                                        int x = 0, y = 0;
                                        int width = 2000, height = 2000;

                                        int quality = 99; // 0 - 100

                                        final String svgContentNoHtmlEntities = svgContent.replace("&nbsp;", " ");

                                        final Path outputImgFile = previewPathFor(uuid, version);

                                        try {
                                            Path inputSvgFile = Files.createTempFile("", ".svg");
                                            Files.write(inputSvgFile,
                                                    svgContentNoHtmlEntities.getBytes(StandardCharsets.UTF_8));
                                            executorService.submit(() -> svgToImage(inputSvgFile,
                                                    outputImgFile,
                                                    x,
                                                    y,
                                                    width,
                                                    height,
                                                    quality));
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }


                                    },
                                    ex -> sendException(exchange, ex)
                            );
                        } else {
                            exchange.setStatusCode(StatusCodes.PRECONDITION_FAILED);
                            exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
                            final JSONObject obj = new JSONObject(Collections.singletonMap("version",
                                    "" + most_recent_version));
                            exchange.getResponseSender().send(obj.toJSONString(), IoCallback.END_EXCHANGE);

                        }
                    },
                    throwable -> sendException(exchange, throwable));

        } catch (Exception e) {
            e.printStackTrace();
            sendException(exchange, e);
        }

    }

    private static JSONObject rowToHypermodel(Row row) throws ParseException {
        return rowToHypermodel(row, null);
    }

    private static JSONObject rowToHypermodel(Row row, JSONParser parser) throws ParseException {
        JSONParser p = parser != null ? parser : new JSONParser(JSONParser.MODE_RFC4627);
        JSONObject js = new JSONObject();
        final String hypermodel_uid = row.getString("hypermodel_uid");
        final Long version = row.getLong("hypermodel_version");
        final Long most_recent_version = row.getLong("most_recent_version");
        final Boolean frozen = row.getBoolean("frozen");
        final String title = row.getString("title");
        final String description = row.getString("description");
        // final String canvas = row.getString("json_content");
        final String graph = row.getString("graph_content");
        final Timestamp created = row.getTimestamp("created");
        final Timestamp updated = row.getTimestamp("version_created");

        final Long[] versions = row.getArray("versions", Long[].class);

        js.put("uuid", hypermodel_uid);
        js.put("version", version.toString());
        js.put("most_recent_version", most_recent_version.toString());
        js.put("frozen", frozen);
        js.put("title", title);
        js.put("description", description);
        // js.put("canvas", canvas);
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_INSTANT;
        js.put("created_at", dateTimeFormatter.format(created.toInstant()));
        js.put("updated_at", dateTimeFormatter.format(updated.toInstant()));
        js.put("graph", p.parse(graph));

        final JSONObject links = new JSONObject();
        links.put("self", "/hypermodels/" + hypermodel_uid + "/" + version);
        final JSONArray verLinks = new JSONArray();
        Arrays.asList(versions).forEach(ver -> {
            verLinks.add("/hypermodels/" + hypermodel_uid + "/" + ver);

        });
        links.put("versions", verLinks);
        js.put("_links", links);

        return js;
    }
}
