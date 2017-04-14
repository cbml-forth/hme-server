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
import gr.forth.ics.cbml.chic.hme.server.execution.ExecutionFramework;
import gr.forth.ics.cbml.chic.hme.server.execution.ExecutionManager;
import gr.forth.ics.cbml.chic.hme.server.execution.Experiment;
import gr.forth.ics.cbml.chic.hme.server.execution.ExperimentRepository;
import gr.forth.ics.cbml.chic.hme.server.modelrepo.*;
import gr.forth.ics.cbml.chic.hme.server.mq.MessageQueueListener;
import gr.forth.ics.cbml.chic.hme.server.mq.Observables;
import gr.forth.ics.cbml.chic.hme.server.utils.DbUtils;
import gr.forth.ics.cbml.chic.hme.server.utils.FileUtils;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.io.IoCallback;
import io.undertow.predicate.Predicate;
import io.undertow.predicate.Predicates;
import io.undertow.security.api.SecurityContext;
import io.undertow.security.idm.Account;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.PredicateHandler;
import io.undertow.server.handlers.resource.*;
import io.undertow.server.handlers.sse.ServerSentEventHandler;
import io.undertow.server.session.InMemorySessionManager;
import io.undertow.server.session.SessionAttachmentHandler;
import io.undertow.server.session.SessionCookieConfig;
import io.undertow.util.*;
import lombok.Value;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.aeonbits.owner.ConfigFactory;
import org.jooq.lambda.tuple.Tuple;
import org.pac4j.core.config.Config;
import org.pac4j.saml.client.SAML2Client;
import org.pac4j.saml.metadata.SAML2MetadataResolver;
import org.pac4j.undertow.account.Pac4jAccount;
import org.pac4j.undertow.context.UndertowWebContext;
import org.pac4j.undertow.handler.ApplicationLogoutHandler;
import org.pac4j.undertow.handler.CallbackHandler;
import org.pac4j.undertow.handler.SecurityHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.undertow.Handlers.routing;

public class HmeServer {
    private static final Logger log =
            LoggerFactory.getLogger(HmeServer.class);


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

        String styles = "svg{background:white;width:5000px;height:5000px}" +
                ".connection-wrap{display:none}" +
                ".marker-vertices{display:none}" +
                ".link-tools{display:none}" +
                ".marker-arrowheads{display:none}" +
                "path.connection{fill:none}" +
                ".port-label {display:none}" +
                "text.label{display:none}" +
                "g.labels{display:none}";

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

        System.err.println(processBuilder.command().stream().collect(Collectors.joining(" ")));

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
            log.error("spawn jpegoptim", e);
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
            log.error("transcode", e);
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

            int quality = 100; // 0 - 100
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
            log.error("previewSvg", e);
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
            log.error("serve static file", e);
            sendException(exchange, e);
        }
    }

    public static Db init() throws IOException {
        previewsDir = Paths.get(System.getProperty("java.io.tmpdir"), "hme-previews");
        log.info("Preview (temp) dir: {}", previewsDir);
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
            final Optional<ChicAccount> chicAccount = ChicAccount.currentUser(exchange);
            if (chicAccount.isPresent()) {
                Sessions.getSession(exchange).setAttribute("chicAccount", chicAccount.get());
            }
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

        System.setProperty ("jsse.enableSNIExtension", "false");

        final HmeServerConfig serverConfig = ConfigFactory.create(HmeServerConfig.class);

        HmeServer.config = serverConfig;

        int port = serverConfig.port();

        final Db database = init();
        final TokenManager tokenManager = new TokenManager(serverConfig.secureTokenService(),
                serverConfig.serviceAccountName(),
                serverConfig.serviceAccountPassword());


        final ModelRepository modelRepository = new ModelRepository(serverConfig.mrServiceUrl(), 3, tokenManager);
        final ExecutionFramework executionFramework = new ExecutionFramework(serverConfig.hfServiceUrl(), 3);
        final ExperimentRepository experimentRepository = new ExperimentRepository(serverConfig.istrServiceUrl(), 3);
        final ExecutionManager executionManager = new ExecutionManager(Files.createTempDirectory(""), tokenManager,
                experimentRepository, modelRepository, executionFramework);

        final String endpoint = config.sparqlRicordo();
        log.info("RICORDO {} ", endpoint);
        final SemanticStore semanticStore = new SemanticStore(endpoint);

        final Config samlConfig = new SamlConfigFactory("https://ssfak.duckdns.org/hme2/",
                config.keystorePath(), config.keystorePassword(),
                config.privateKeyPassword(), config.identityProviderMetadataPath()).build();

        MessageQueueListener queueListener = new MessageQueueListener(serverConfig, database);
        queueListener.connect(true);
//        queueListener.observables()
//                .modelMessages()
//                .subscribe(message -> log.info("** {} ",message));
//        queueListener.observables()
//                .modelMessages()
//                .subscribe(message -> log.info("// {} ",message));


        final RoutingHandler apiRoutes = routing(true)
                .get("/models", exchange -> exchange.dispatch(() -> getAllModels(exchange, tokenManager,
                        database, modelRepository, semanticStore)))
                .get("/models/{id}", exchange -> exchange.dispatch(() ->
                     getParam(exchange, "id")
                             .map(Long::valueOf)
                             .map(RepositoryId::new)
                             .ifPresent(id -> modelRepository.getModel(id, "crafsrv")
                                         .thenApply(Model::toJSON)
                                         .whenComplete((js, throwable) -> {
                                             if (throwable != null)
                                                 sendException(exchange, throwable);
                                             else {
                                                 exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
                                                 exchange.getResponseSender().send(js.toJSONString(), IoCallback.END_EXCHANGE);
                                             }
                                 }))))

                .get("/hypermodels", exchange -> exchange.dispatch(() -> get_hypermodels(exchange, database)))
                .post("/hypermodels", exchange -> exchange.dispatch(() -> save_hypermodel(exchange, database)))
                .get("/hypermodels/{uuid}/{version}",
                        exchange -> exchange.dispatch(() -> get_hypermodel(exchange, database)))
                .get("/hypermodels/{uuid}", exchange -> exchange.dispatch(() -> get_hypermodel(exchange, database)))
                .put("/publishedhypermodels/{uuid}",
                        exchange -> exchange.dispatch(() -> publishHypermodelAndRun(exchange, database, modelRepository, executionManager)));

        // The Api Routes are validated against an authenticated user and
        // that includes an "X-Requested-By" header. The latter is easy way to
        // mitigate CSRF attacks for REST services.
        // See OWASP guidelines at https://goo.gl/R0csc3
        final Predicate authnPredicate = ChicAccount.authnPredicate();
        final Predicate xhrRequestPredicate = (HttpServerExchange e) -> e.getRequestHeaders().contains("X-Requested-By");
        final Predicate apiRoutesPredicate = Predicates.and(authnPredicate, xhrRequestPredicate);

        //final HttpHandler corsHandler = new CORSHandler().wrap(apiRoutes);

        final String editorUrl = BASE_PATH + "/static/html/";
        final HttpHandler loginHandler = createLoginHandler("url", editorUrl);
        final HttpHandler redirectToLoginHandler = createLoginRedirectHandler("url", BASE_PATH + "/login");
        final ResourceHandler resourceHandler = new ResourceHandler(
                new PathResourceManager(Paths.get(serverConfig.staticDir()), 1024))
                .setWelcomeFiles("index.html");
        HttpHandler rootHandler = Handlers.path()
                // Redirect root path to /static to serve the index.html by default
                .addExactPath("/", new PredicateHandler(authnPredicate,
                        Handlers.redirect(editorUrl), redirectToLoginHandler))

                .addPrefixPath("/h", new PredicateHandler(authnPredicate,
                        routing(true).get("{uuid}",
                                exchange -> redirect(exchange, editorUrl +"#" + exchange.getQueryParameters().get("uuid").getFirst())),
                                redirectToLoginHandler))

                // Serve all static files from a folder
                .addPrefixPath("/static", new PredicateHandler(authnPredicate,
                        resourceHandler, redirectToLoginHandler))


                // Provide image previews for the hypermodels: No authentication is required
                // in order to be easily integrated (e.g. in the reports that CRAF generates)
                .addExactPath("/preview",
                        exchange -> exchange.dispatch(() -> preview_hypermodel(exchange, database)))

                // REST API path
                .addPrefixPath("/api", new PredicateHandler(apiRoutesPredicate, apiRoutes, HmeServer::apiError401))

                // server-sent events, real time monitoring (e.g. for hypermodels execution status)
                .addExactPath("/sse", new PredicateHandler(authnPredicate,
                        exchange -> exchange.dispatch(() -> monitor(exchange, queueListener.observables())),
                        HmeServer::apiError401))

                .addExactPath("/login", SecurityHandler.build(loginHandler, samlConfig, "SAML2Client"))

                // Security related endpoints:
                .addExactPath("/metadata", exchange -> HmeServer.samlMetadata(exchange, samlConfig))
                .addExactPath("/callback", CallbackHandler.build(samlConfig, null, true))
                .addExactPath("/logout", new ApplicationLogoutHandler(samlConfig, BASE_PATH + "/"));


        final SessionCookieConfig sessionConfig = new SessionCookieConfig();
        sessionConfig.setCookieName("HMESESSIONID");
        sessionConfig.setPath(FileUtils.endWithSlash(BASE_PATH));
        sessionConfig.setHttpOnly(true);
        final InMemorySessionManager sessionManager = new InMemorySessionManager("SessionManager", serverConfig.maxSessions());
        final SessionAttachmentHandler sessionHandler =
                new SessionAttachmentHandler(Handlers.path().addPrefixPath(BASE_PATH, rootHandler),
                        sessionManager,
                        sessionConfig);
        Undertow server = Undertow.builder()
                .addHttpListener(port, HmeServer.config.hostname())
                .setWorkerThreads(20)
                .setHandler(sessionHandler).build();
        log.info("All ready.. Start listening on {}:{}", HmeServer.config.hostname(), port);
        server.start();
    }

    private static Optional<String> getParam(HttpServerExchange exchange, String paramName)
    {

        final Map<String, Deque<String>> queryParameters = exchange.getQueryParameters();
        if (queryParameters.containsKey(paramName))
            return Optional.of(queryParameters.get(paramName).getFirst());
        return Optional.empty();
    }

    private static Optional<UUID> getUuidParam(HttpServerExchange exchange, String paramName)
    {
        return getParam(exchange, paramName)
                .flatMap(param -> {
                    try {
                        UUID uuid = UUID.fromString(param);
                        return Optional.of(uuid);
                    } catch (IllegalArgumentException ex) {
                        return Optional.empty();
                    }
                });
    }

    private static void publishHypermodelAndRun(HttpServerExchange exchange,
                                                Db database,
                                                ModelRepository modelRepository,
                                                ExecutionManager executionManager)
    {
        final Optional<UUID> uuidOpt = getUuidParam(exchange, "uuid");
        if (!uuidOpt.isPresent()) {
            sendNotFound(exchange, "Hypermodel not found!");
            return;
        }
        Optional<String> actAsOpt = userId(exchange);
        if (!actAsOpt.isPresent()) {
            sendError(exchange, "Not authenticated", StatusCodes.UNAUTHORIZED);
            return;
        }

        final UUID hypermodelUuid = uuidOpt.get();
        final String actAs = actAsOpt.get();

        exchange.startBlocking();

        String workflowDescription;
        long version = 0;
        Boolean isStronglyCoupled;
        List<ModelParameter> inputs, outputs;
        try {

            JSONParser parser = new JSONParser(JSONParser.MODE_RFC4627);
            final JSONObject o = (JSONObject) parser.parse(exchange.getInputStream());
            workflowDescription = o.getAsString("xmml");
            version = Long.valueOf(o.getAsString("version"));
            isStronglyCoupled = (Boolean) o.getOrDefault("isStronglyCoupled", Boolean.FALSE);
            JSONArray ins = (JSONArray) o.get("inputs");
            inputs = ins.stream()
                    .map(JSONObject.class::cast)
                    .map(js -> {
                        final ModelParameter param = ModelParameter.fromJson((JSONObject) js.get("param"));
                        final String value = js.getAsString("value");
                        return param.withValue(value);
                    })
                    .collect(Collectors.toList());

            JSONArray outs = (JSONArray) o.get("outputs");
            outputs = outs.stream().map(JSONObject.class::cast).map(ModelParameter::fromJson).collect(Collectors.toList());
        } catch (IOException | ParseException e) {
            sendException(exchange, e);
            return;
        }

        publishHypermodel(database, modelRepository, hypermodelUuid, version, actAs,
                workflowDescription, inputs, outputs)
                .thenCompose(hypermodel -> {
                    final RepositoryId repoId = hypermodel.getPublishedRepoId().get();
                    System.err.println("--> Running " + repoId);
                    return executionManager.runHypermodel(repoId, "dfsfs", "dsf", inputs, actAs)
                            .thenApply(experiment -> Tuple.tuple(hypermodel, experiment));
                })
                .whenComplete((tuple2, ex) -> {
                    if (ex != null) {
                        sendException(exchange, ex);
                        return;
                    }
                    final Hypermodel hypermodel = tuple2.v1();
                    final Experiment experiment = tuple2.v2();
                    final RepositoryId experimentId = experiment.getId();
                    final Experiment.EXP_RUN_STATE status = experiment.getStatus();
                    final String jsonString = experiment.toJson().toJSONString();
                    exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send(jsonString, IoCallback.END_EXCHANGE);

                    DbUtils.queryDb(database,
                            "INSERT INTO experiments(experiment_id,hypermodel_uid,hypermodel_version,workflow_uuid, status,data)" +
                                    " VALUES($1,$2,$3,$4,$5,$6)",
                            Arrays.asList(experimentId.getId(), hypermodel.getUuid(),
                                    hypermodel.getVersion(), experiment.getWorkflow_uuid(),
                                    status.toString(), jsonString));


                });
    }

    private static CompletableFuture<Hypermodel> publishHypermodel(Db database, ModelRepository modelRepository, UUID hypermodelUuid, long version, String actAs, String workflowDescription, List<ModelParameter> inputs, List<ModelParameter> outputs) {
        /*db_get_hypermodel(database, hypermodelUuid, Optional.empty(), actAs)
                .whenComplete((rs, throwable)-> {
                    if (throwable != null) {
                        sendException(exchange, throwable);
                        return;
                    }
                    if (!rs.isPresent()) {
                        sendNotFound(exchange, "hypermodel " + hypermodelUuid + " not found");
                        return;
                    }
                    final Hypermodel hm = rs.get();
                    modelRepository.storeHyperModel(hm, inputs, outputs, workflowDescription, actAs)
                            .thenApply(model -> hm.withRepoId(model.getId()))
                            .thenCompose(hypermodel -> db_publish_hypermodel(database, hypermodel, workflowDescription))
                            .whenComplete((hypermodel, ex) -> {
                                if (ex != null) {
                                    sendException(exchange, ex);
                                    return;
                                }
                                JSONObject js = hypermodel.toModel().toJSON();
                                exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
                                exchange.getResponseSender().send(js.toJSONString(), IoCallback.END_EXCHANGE);
                            });
                }); */
        return  db_get_hypermodel(database, hypermodelUuid, Optional.of(version), actAs)
                .thenApply(opt  -> {
                    if (!opt.isPresent()) {
                        throw new RuntimeException("hypermodel " + hypermodelUuid + " not found");
                    }
                    return opt.get();
                })
                .thenCompose(hm -> modelRepository.storeHyperModel(hm, inputs, outputs, workflowDescription, actAs)
                        .thenApply(model -> hm.withRepoId(model.getId()))
                        .thenCompose(hypermodel -> db_publish_hypermodel(database, hypermodel, workflowDescription)));
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

    private static void getAllModelsLocal(HttpServerExchange exchange, TokenManager tokMgr,
                                     Db db, ModelRepository modelRepository,
                                     SemanticStore semanticStore) {
        System.err.println("-> getAllModels");

        ChicAccount.currentUser(exchange).map(Object::toString).ifPresent(System.out::println);
        ChicAccount.currentUser(exchange).map(ChicAccount::attrsToString).ifPresent(System.out::println);
        Optional<String> actAs = userId(exchange);
        final CompletableFuture<List<Map<String, String>>> annotationsOfModels = // getAnnotationsOfModels(semanticStore);
        CompletableFuture.completedFuture(Collections.emptyList());
        tokMgr.getDelegationToken(modelRepository.AUDIENCE, actAs.isPresent() ? actAs.get() : null)
                .thenApply(token -> {

                    try (FileInputStream fis = new FileInputStream("/Users/ssfak/Documents/Projects/CHIC/WP10/editor/chic-hme/resources/public/models.json")) {
                        final JSONParser jsonParser = new JSONParser(JSONParser.MODE_RFC4627);
                        final JSONArray objects = (JSONArray) jsonParser.parse(fis);

                        return objects.stream()
                                .map(JSONObject.class::cast)
                                .map(json -> {
                                    json.put("perspectives", new JSONObject());
                                    json.put("uuid", UUID.randomUUID().toString());
                                    return json;
                                })
                                .collect(Collectors.toList());

                    } catch (IOException | ParseException e) {
                        e.printStackTrace();
                        return new JSONArray();
                    }
                })
                .whenComplete((list, throwable) -> {

                    if (throwable != null)
                        sendException(exchange, throwable);
                    else {
                        exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
                        final JSONArray objects = new JSONArray();
                        objects.addAll(list);
                        exchange.getResponseSender().send(objects.toJSONString(), IoCallback.END_EXCHANGE);
                    }
                });
    }

    private static void getAllModels(HttpServerExchange exchange, TokenManager tokMgr,
                                     Db db, ModelRepository modelRepository,
                                     SemanticStore semanticStore) {
        System.err.println("-> getAllModels");

        ChicAccount.currentUser(exchange).map(ChicAccount::attrsToString).ifPresent(System.err::println);
        Optional<String> actAs = userId(exchange);
        final CompletableFuture<List<Map<String, String>>> annotationsOfModels = getAnnotationsOfModels(semanticStore)
                .exceptionally(ex -> {
                    log.error("Getting annotations ", ex);

                    return Collections.<Map<String, String>>emptyList();
                });
        final CompletableFuture<Map<UUID, Integer>> mapCompletableFuture = countHypermodelsPerModel(db);
        final CompletableFuture<Map<UUID, RepositoryId>> publishedHypermodelsFut = publishedHypermodels(db);
        final CompletableFuture<List<Model>> allModels = tokMgr.getDelegationToken(modelRepository.AUDIENCE, actAs.isPresent() ? actAs.get() : null)
                .thenCompose(modelRepository::getAllModels);

        CompletableFuture.allOf(allModels, publishedHypermodelsFut, mapCompletableFuture, annotationsOfModels)
                .thenApply(aVoid -> {
                    final List<Model> models = allModels.join();
                    final Map<UUID, RepositoryId> published = publishedHypermodelsFut.join();
                    final Map<UUID, Integer> counts = mapCompletableFuture.join();
                    final List<Map<String, String>> annotations = annotationsOfModels.join();

                    final Map<String, List<Map<String, String>>> annsPerModel = annotations.stream().collect(Collectors.groupingBy(m -> m.get("uuid")));

                    final List<JSONObject> jsonObjects = models.stream().map(model -> {
                        final JSONObject json = model.toJSON();
                        final UUID uuid = model.getUuid();
                        final Integer usage = counts.getOrDefault(uuid, 0);
                        json.put("usage", usage);
                        if (published.containsKey(uuid)) {
                            json.put("published_id", published.get(uuid).toJSON());
                        }
                        if (annsPerModel.containsKey(uuid.toString())) {
                            final Map<String, String> m = annsPerModel.get(uuid.toString()).get(0);
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
                        return json;

                    }).collect(Collectors.toList());
                    final JSONArray array = new JSONArray();
                    array.addAll(jsonObjects);
                    return array;
                })
        /*
                .thenCombine(mapCompletableFuture, (models, counts) -> models.stream().map(model -> {
                    final JSONObject json = model.toJSON();
                    final UUID uuid = model.getUuid();
                    final Integer usage = counts.getOrDefault(uuid, 0);
                    json.put("usage", usage);
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
                }) */
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


    private static Optional<String> userId(final HttpServerExchange exchange) {
        return ChicAccount.currentUser(exchange).map(ChicAccount::getUsername);
    }

    private static void get_hypermodels(final HttpServerExchange exchange, Db db) {
        final String user_id = userId(exchange).get();

        final long start = System.currentTimeMillis();

        final String sql =
                "SELECT * FROM hypermodel_versions_vw WHERE user_id=$1 AND hypermodel_version=most_recent_version";

        final Consumer<Throwable> onError = throwable -> sendException(exchange, throwable);
        db.query(sql, Arrays.asList(user_id),
                rs -> {
                    final long stop = System.currentTimeMillis();
                    double elapsedTime = (stop - start) / 1000.0;
                    JSONParser p = new JSONParser(JSONParser.MODE_RFC4627);
                    JSONArray l = new JSONArray();
                    rs.forEach(row -> {
                        try {
                            final Hypermodel hypermodel = rowToHypermodel(row, p);
                            JSONObject js = hypermodel.toJson();
                            l.add(js);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    });
                    exchange.getResponseHeaders().add(HttpString.tryFromString("Server-Timing"), String.format("db=%f ;Database", elapsedTime));
                    exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send(l.toJSONString(), IoCallback.END_EXCHANGE);
                },
                onError);
    }

    private static CompletableFuture<Optional<Hypermodel>> db_get_hypermodel(Db db,
                                          UUID hypermodelUuid,
                                          Optional<Long> version,
                                          final String userId)
    {
        final String sql = "SELECT * FROM hypermodel_versions_vw" +
                " WHERE user_id=$1 AND hypermodel_uid=$2 AND hypermodel_version=" +
                (version.isPresent() ? "$3" : "most_recent_version");

        final List params = version.isPresent()
                ? Arrays.asList(userId, hypermodelUuid, version.get())
                : Arrays.asList(userId, hypermodelUuid);

        return DbUtils.queryOneDb(db, sql, params)
                .thenApply(opt -> opt.map(HmeServer::rowToHypermodel));
    }

    private static void get_hypermodel(final HttpServerExchange exchange, Db db) {
        final String user_id = userId(exchange).get();
        final Optional<UUID> uuidOptional = getUuidParam(exchange, "uuid");
        if (!uuidOptional.isPresent()) {
            sendError(exchange, "Hypermodel was not found!", StatusCodes.NOT_FOUND);
            return;
        }
        final Optional<Long> versionOpt = getParam(exchange, "version").map(Long::valueOf);

        final UUID uuid = uuidOptional.get();
        db_get_hypermodel(db, uuid, versionOpt, user_id)
                .whenComplete((hmOpt, throwable) -> {
                    if (throwable != null) {
                        sendException(exchange, throwable);
                        return;
                    }
                    if (!hmOpt.isPresent()) {
                        sendError(exchange,
                                "Hypermodel '" + uuid + "' (version=" + versionOpt.map(Object::toString).orElse("") + ") was not found!",
                                StatusCodes.NOT_FOUND);
                        return;

                    }
                    final Hypermodel hm = hmOpt.get();

                    final Long version = hm.getVersion();
                    final Instant last_update = hm.getUpdatedAt();
                    String etag = String.format("%d", version);
                    final ETag eTag = new ETag(false, etag);


                    final HeaderMap responseHeaders = exchange.getResponseHeaders();
                    responseHeaders.add(Headers.ETAG, eTag.toString());
                    if (ETagUtils.handleIfNoneMatch(exchange, eTag, eTag.isWeak())) {
                        JSONObject js = hm.toJson();
                        // Allow caching for 2 hours:
                        // (https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control)
                        responseHeaders.add(Headers.CACHE_CONTROL, "max-age:7200, private, must-revalidate");
                        responseHeaders.add(Headers.CONTENT_TYPE, "application/json");
                        responseHeaders.add(Headers.LAST_MODIFIED, DateUtils.toDateString(Date.from(last_update)));
                        exchange.getResponseSender().send(js.toJSONString(), IoCallback.END_EXCHANGE);
                    } else {
                        exchange.setStatusCode(StatusCodes.NOT_MODIFIED);
                        exchange.endExchange();
                    }


                });
    }
    private static CompletableFuture<Hypermodel> db_publish_hypermodel(Db db,
                                                                 Hypermodel hm,
                                                                 String workflowDescription)
    {
        assert hm.getPublishedRepoId().isPresent();

        final Long repoID = hm.getPublishedRepoId().get().getId();
        return DbUtils.queryOneDb(db,
                "INSERT INTO published_versions(hypermodel_uid,hypermodel_version,repository_id,xmml)" +
                        " VALUES($1,$2,$3,$4)" +
                        " ON CONFLICT (hypermodel_uid) DO UPDATE SET hypermodel_version=$2, repository_id=$3, xmml=$4" +
                        " RETURNING hypermodel_version",
                Arrays.asList(hm.getUuid(), hm.getVersion(), repoID, workflowDescription))
                .thenApply(row -> hm);
    }

    private static Path previewPathFor(final UUID hypermodel_uid, long version)
    {
        Objects.requireNonNull(hypermodel_uid);
        final String outputImgFilename = hypermodel_uid.toString() + "-" + version + ".jpg";
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
        final String user_id = userId(exchange).get();

        final Optional<UUID> uuidOptional = getUuidParam(exchange, "hmid");
        if (!uuidOptional.isPresent()) {
            sendError(exchange, "Hypermodel not found!", StatusCodes.NOT_FOUND);
            return;
        }

        final UUID uuid = uuidOptional.get();
        final Long version = getParam(exchange, "ver").map(Long::valueOf).orElse(0L);

//        String etag = queryParameters.entrySet().stream()
//                .map(entry -> entry.getKey() + ":" + entry.getValue().getFirst())
//                .collect(Collectors.joining("/"));

        Path outputImgFile = previewPathFor(uuid, version);
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

        final String sql = "SELECT svg_content FROM hypermodel_versions_vw" +
                " WHERE user_id=$1 AND hypermodel_uid=$2 AND hypermodel_version=" +
                (version > 0 ? "$3" : "most_recent_version");

        final List params = version > 0 ? Arrays.asList(user_id, uuid, version) : Arrays.asList(user_id, uuid);
        DbUtils.queryOneDb(db, sql, params)
                .whenComplete((rs, throwable) -> {
                    if (throwable != null) {
                        sendException(exchange, throwable);
                        return;
                    }
                    if (!rs.isPresent()) {
                        sendError(exchange,
                                "Hypermodel '" + uuid + "' (version=" + version + ") was not found!",
                                StatusCodes.NOT_FOUND);
                        return;
                    }

                    final Row row = rs.get();
                    final String svgContent = row.getString("svg_content");
                    final String svgContentNoHtmlEntities = svgContent.replace("&nbsp;", " ");


                    try {
                        Path inputSvgFile = Files.createTempFile("", ".svg");
                        Files.write(inputSvgFile, svgContentNoHtmlEntities.getBytes(StandardCharsets.UTF_8));
                        executorService.submit(() -> previewSvg(exchange, inputSvgFile, outputImgFile, eTag));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                });
    }

    static void sendException(final HttpServerExchange exchange, final Throwable throwable) {
        if (throwable.getCause() != null)
            sendException(exchange, throwable.getCause());
        else
            sendException(exchange, throwable, StatusCodes.INTERNAL_SERVER_ERROR);
    }

    static void sendException(final HttpServerExchange exchange, final Throwable throwable, int code) {
//        throwable.printStackTrace();
        log.error("Error", throwable);
        sendError(exchange, throwable.getMessage(), code);
    }

    static void sendNotFound(final HttpServerExchange exchange, final String msg) {
        sendError(exchange, msg, StatusCodes.NOT_FOUND);
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

    @Value
    static class ModelUsageInfo {
        UUID model_uuid;
        RepositoryId published_id;
        int usage;
        Optional<UUID> hypermodel_uuid;

    }
    private static CompletableFuture<Map<UUID, Integer>> countHypermodelsPerModel(final Db db) {
        final String sql = "select model_uuid, count(*)::int4" +
                " FROM hypermodel_versions_models JOIN recent_versions_vw USING (hypermodel_uid)" +
                " WHERE hypermodel_version = most_recent_version GROUP BY model_uuid";
        return DbUtils.queryDb(db, sql)
                .thenApply(rs -> {
                    final HashMap<UUID, Integer> map = new HashMap<>();
                    for (Row r : rs) {
                        final UUID model_uuid = UUID.fromString(r.getString(0));
                        final Integer cnt = r.getInt(1);
                        map.put(model_uuid, cnt);
                    }
                    return map;
                });
    }
    private static CompletableFuture<Map<UUID, RepositoryId>> publishedHypermodels(final Db db) {
        final String sql =
                "SELECT hypermodel_uid::text AS model_uuid, repository_id" +
                " FROM published_versions";
        return DbUtils.queryDb(db, sql)
                .thenApply(rs -> {
                    final HashMap<UUID, RepositoryId> map = new HashMap<>();
                    for (Row r : rs) {
                        final UUID model_uuid = UUID.fromString(r.getString(0));
                        final Long cnt = r.getLong(1);
                        map.put(model_uuid, new RepositoryId(cnt));
                    }
                    return map;
                });
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

        final String user_id = userId(exchange).get();
        final Consumer<Throwable> onError = throwable -> sendException(exchange, throwable);
        exchange.startBlocking();
        try {
            JSONParser parser = new JSONParser(JSONParser.MODE_RFC4627);
            final JSONObject o = (JSONObject) parser.parse(exchange.getInputStream());
            final UUID uuid = UUID.fromString(o.getAsString("uuid"));
            final String title = o.getAsString("title");
            final String description = o.getAsString("description");
            final String canvas = o.getAsString("canvas");
            final String svgContent = o.getOrDefault("svg_content", "").toString();
            final JSONObject graph = (JSONObject) o.get("graph");
            final boolean isStronglyCoupled = (Boolean) o.getOrDefault("isStronglyCoupled", Boolean.TRUE);

            // System.err.println("GOT : " + o.toJSONString());

            final boolean frozen = (Boolean) o.getOrDefault("frozen", Boolean.FALSE);

            final String sql = "WITH upd AS " +
                    "(INSERT INTO hypermodels(hypermodel_uid,user_id)" +
                    " VALUES($1,$2) ON CONFLICT (hypermodel_uid) DO UPDATE SET updated=now()" +
                    " RETURNING *)" +
                    " SELECT MAX(hypermodel_version) most_recent_version" +
                    " FROM hypermodel_versions JOIN upd USING (hypermodel_uid)";
            long start = System.currentTimeMillis();
            db.query(sql, Arrays.asList(uuid, user_id),
                    rs -> {

                        final Long most_recent_version = rs.row(0).getLong(0);

                        final ETag eTag = hypermodelEtag(most_recent_version);

                        if (rs.size() == 0 || ETagUtils.handleIfMatch(exchange, eTag, eTag.isWeak())) {
                            db.query(
                                    "INSERT INTO hypermodel_versions(hypermodel_uid, title, description, svg_content, json_content, graph_content, frozen, strongly_coupled)" +
                                            " VALUES($1, $2, $3, $4, $5, $6, $7, $8)" +
                                            " RETURNING hypermodel_version",
                                    Arrays.asList(uuid, title, description, svgContent, canvas, graph.toJSONString(), frozen, isStronglyCoupled),
                                    rs1 -> {
                                        long stop = System.currentTimeMillis();
                                        double elapsedTime = (stop - start) / 1000.0;

                                        final Long version = rs1.row(0).getLong(0);
                                        final ETag eTag2 = hypermodelEtag(version);

                                        exchange.setStatusCode(StatusCodes.CREATED);
                                        exchange.getResponseHeaders()
                                                .add(HttpString.tryFromString("Server-Timing"), String.format("db=%f ;Database", elapsedTime))
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
                                    onError
                            );
                        } else {
                            exchange.setStatusCode(StatusCodes.PRECONDITION_FAILED);
                            exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
                            final JSONObject obj = new JSONObject(Collections.singletonMap("version",
                                    "" + most_recent_version));
                            exchange.getResponseSender().send(obj.toJSONString(), IoCallback.END_EXCHANGE);

                        }
                    },
                    onError);

        } catch (Exception e) {
            onError.accept(e);
        }

    }

    private static JSONObject rowToHypermodelJson(Row row) throws ParseException {
        final Hypermodel hypermodel = rowToHypermodel(row, null);
        return hypermodel.toJson();
    }

    private static Hypermodel rowToHypermodel(Row row) {
        return rowToHypermodel(row, null);
    }
    private static Hypermodel rowToHypermodel(Row row, JSONParser parser) {
        JSONParser p = parser != null ? parser : new JSONParser(JSONParser.MODE_RFC4627);
        final String hypermodel_uid = row.getString("hypermodel_uid");
        final Long version = row.getLong("hypermodel_version");
        final Boolean frozen = row.getBoolean("frozen");
        final Boolean isStronglyCoupled = row.getBoolean("strongly_coupled");
        final String title = row.getString("title");
        final String description = row.getString("description");
        // final String canvas = row.getString("json_content");
        final String graph = row.getString("graph_content");
        final Timestamp created = row.getTimestamp("created");
        final Timestamp updated = row.getTimestamp("version_created");

        final Long[] versions = row.getArray("versions", Long[].class);
        final Optional<RepositoryId> repository_id = Optional.ofNullable(row.getLong("repository_id")).map(RepositoryId::new);

        JSONObject graphJson = new JSONObject();
        try {
            graphJson = (JSONObject) p.parse(graph);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Hypermodel hm = Hypermodel.builder()
                .uuid(UUID.fromString(hypermodel_uid))
                .version(version)
                .isFrozen(frozen)
                .isStronglyCoupled(isStronglyCoupled)
                .name(title)
                .description(description)
                .createdAt(created.toInstant())
                .updatedAt(updated.toInstant())
                .graph(graphJson)
                .allVersions(Arrays.asList(versions))
                .publishedRepoId(repository_id.orElse(null))
                .build();
        return hm;
    }



    static void monitor(HttpServerExchange exchange, Observables observables) {
        final String userId = ChicAccount.currentUser(exchange).map(ChicAccount::getUserId).get();

        System.out.printf("[%s] monitor_experiment\n", userId);
        try {
            exchange.getResponseHeaders().put(Headers.CACHE_CONTROL, "no-cache");
            // For proxying by nginx (https://www.nginx.com/resources/wiki/start/topics/examples/x-accel/#x-accel-buffering):
            exchange.getResponseHeaders().put(HttpString.tryFromString("X-Accel-Buffering"), "no");
            new ServerSentEventHandler(new MonitorConnectionHandler(userId, observables.executionMessages()))
                    .handleRequest(exchange);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
