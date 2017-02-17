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
package gr.forth.ics.cbml.chic.hme.server.modelrepo;

import gr.forth.ics.cbml.chic.hme.server.SAMLToken;
import gr.forth.ics.cbml.chic.hme.server.TokenManager;
import gr.forth.ics.cbml.chic.hme.server.WebApiServer;
import gr.forth.ics.cbml.chic.hme.server.utils.FutureUtils;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import net.minidev.json.JSONObject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class ModelRepository implements AutoCloseable {

//    public final static String AUDIENCE = "https://mr.chic-vph.eu/";
    public final static String AUDIENCE = "http://147.102.5.177/";
    final static String BASE_URI = AUDIENCE + "model_app/";
    final WebApiServer apiServer;
    final TokenManager tokenManager;

    public ModelRepository(int concurrency, TokenManager tokenManager) {
        this.apiServer = new WebApiServer(concurrency);
        this.tokenManager = tokenManager;
    }

    public WebApiServer apiServer() {
        return this.apiServer;
    }

    enum Model_Cache {

        INSTANCE;


        private final ConcurrentHashMap<RepositoryId, Model> cache_;

        Model_Cache() {
            this.cache_ = new ConcurrentHashMap<>();
        }

        public boolean contains(final RepositoryId modelId) {

            final boolean b = this.cache_.containsKey(modelId);
            return b;
        }

        public Model get(final RepositoryId modelId) {
            return this.cache_.get(modelId);
        }

        public void put(final RepositoryId modelId, Model m) {
            this.cache_.put(modelId, m);
        }

        public void clear() {
            this.cache_.clear();
        }
    }
    private CompletableFuture<JSONObject> getModelParamsJSON(RepositoryId modelId, final SAMLToken token) {
        final String url = BASE_URI + "getParametersByToolId/";
        return this.apiServer.getJsonAsync(url, token, Collections.singletonMap("tool_id", modelId.toString()))
                .thenApply(JSONObject.class::cast);
    }

    private CompletableFuture<JSONObject> getModelJSON(RepositoryId modelId, final SAMLToken token) {
        final String url = BASE_URI + "getToolById/";
        return this.apiServer.getJsonAsync(url, token, Collections.singletonMap("id", modelId.toString()))
                .thenApply(JSONObject.class::cast);
    }


    @Value
    public static class File {
        String id;
        String sha;
        String description;
        String kind;

        public static File fromJSON(final JSONObject jsonFile) {
            val id = jsonFile.getAsString("id");
            val sha = jsonFile.getAsString("sha1sum");
            val description = jsonFile.getAsString("description");
            val kind = jsonFile.getAsString("kind");
            return new File(id, sha, description, kind);
        }
    }

    public void clearCache()
    {
        Model_Cache.INSTANCE.clear();
    }

    public CompletableFuture<List<File>> getModelFilesJSON(final RepositoryId modelId,
                                                           final SAMLToken token) {
        final String url = BASE_URI + "getFilesByToolId/";
        HashMap<String, String> qs = new HashMap<>();
        qs.put("tool_id", modelId.getId()+"");
        return this.apiServer.getJsonAsync(url, token, qs)
                .thenApplyAsync(jsonAware -> ((JSONObject) jsonAware).values()
                        .stream()
                        .map(JSONObject.class::cast)
                        .map(File::fromJSON)
                        .collect(Collectors.toList()));
    }

    public CompletableFuture<Optional<File>> getWorkflowFileForModel(final RepositoryId modelId,
                                                                     final SAMLToken token)
    {
        return this.getModelFilesJSON(modelId, token)
                .thenApply(list -> list.stream()
                        .filter(f -> WorkflowKind.fromString(f.kind) != WorkflowKind.OTHER)
                        .findAny());
    }

    public static String fileUrl(final String file_id) {
        return BASE_URI + "getFileById/?id=" + file_id;
    }

    public CompletableFuture<java.io.File> downloadFile(final String fileId,
                                                        final SAMLToken token) throws IOException
    {
        final String url = BASE_URI + "getFileById/";
        HashMap<String, String> qs = new HashMap<>();
        qs.put("id", fileId);


        final Path tempFile = Files.createTempFile("", ".data");
        final FileOutputStream outputStream = new FileOutputStream(tempFile.toFile());

        return this.apiServer.downloadContent(url, token, qs, outputStream)
                .thenApply(__ -> tempFile.toFile());
    }


    private CompletableFuture<Model> fillModelParams(final Model model,
                                                     final SAMLToken token)
    {
        return getModelParamsJSON(model.getId(), token)
                .thenApply(js -> {
                    final Map<Boolean, List<ModelParameter>> map = js.values().stream()
                            .map(JSONObject.class::cast)
                            .map(ModelParameter::fromJson)
                            .sorted(Comparator.comparing(modelParameter -> modelParameter.getId().getId()))
                            .collect(Collectors.partitioningBy(ModelParameter::isOutput));
                    model.setOutputs(map.get(true));
                    model.setInputs(map.get(false));
                    return model;
                })
                .whenComplete((m, ex) -> {
                    if (m != null)
                        cacheModel(m);
                });
    }


    public CompletableFuture<Model> getModel(RepositoryId modelId,
                                             final String actAs)
    {

        if (Model_Cache.INSTANCE.contains(modelId))
            return CompletableFuture.completedFuture(Model_Cache.INSTANCE.get(modelId));
        return tokenManager.getDelegationToken(this.AUDIENCE, actAs)
                .thenCompose(samlToken -> getModel(modelId, samlToken));
    }
    public CompletableFuture<Model> getModel(RepositoryId modelId,
                                             final SAMLToken token)
    {

        if (Model_Cache.INSTANCE.contains(modelId))
            return CompletableFuture.completedFuture(Model_Cache.INSTANCE.get(modelId));
        return getModelJSON(modelId, token)
                .thenApply(js -> parseModelJson(modelId, js))
                .thenCompose(model -> fillModelParams(model, token));
    }

    private static Model parseModelJson(RepositoryId modelId, JSONObject modelJson) {
        final String name = modelJson.getAsString("title");
        final String description = modelJson.getAsString("description");
        System.err.printf("== %s UUID = %s\n", modelId, modelJson.getAsString("uuid"));
        final UUID uuid = UUID.fromString(modelJson.getAsString("uuid"));
        final Number strongly_coupled = modelJson.getAsNumber("strongly_coupled");
        final Number isFrozenNum = modelJson.getAsNumber("freezed");
        final boolean isStronglyCoupled = strongly_coupled != null && strongly_coupled.intValue() != 0;
        final boolean isFrozen = isFrozenNum != null && isFrozenNum.intValue() != 0;
        return new Model(modelId, name, description, uuid, isStronglyCoupled, isFrozen);
    }


    private void cacheModel(final Model m)
    {
        Model_Cache.INSTANCE.put(m.getId(), m);
        log.info("Caching model {}", m.getId());
    }

    public CompletableFuture<List<Model>> getAllModels(final SAMLToken token) {
        final String url = BASE_URI + "getAllTools/";
        return this.apiServer.getJsonAsync(url, token)
                .thenApply(JSONObject.class::cast)
                .thenCompose(jsonObject -> {
                    final List<CompletableFuture<Model>> futureList =
                            jsonObject.values().stream()
                                    .map(JSONObject.class::cast)
                                    .map(js -> {
                                        RepositoryId modelId =  RepositoryId.fromJsonObj(js, "id");
                                        boolean isCached = Model_Cache.INSTANCE.contains(modelId);
                                        if (isCached) {
                                            final Model model = Model_Cache.INSTANCE.get(modelId);
                                            return CompletableFuture.completedFuture(model);
                                        } else {
                                            final Model model = parseModelJson(modelId, js);
                                            return this.fillModelParams(model, token);
                                        }
                                    })
                                    .collect(Collectors.toList());
                    return FutureUtils.sequence(futureList);
                });
    }

    public CompletableFuture<Model> storeHyperModel(Hypermodel hypermodel,
                                                    List<ModelParameter> inputs,
                                                    List<ModelParameter> outputs,
                                                    String workflowDescription,
                                                    String actAsUser) {
        return this.tokenManager
                .getDelegationToken(this.AUDIENCE, actAsUser)
                .thenCompose(token ->
                        this.storeHyperModelAsTool(hypermodel, token)
                                .thenApply(model -> {
                                    final WorkflowKind kind = hypermodel.kind();
                                    //String fileTitle = String.valueOf((title + " (" + version + ")." + kind).hashCode());
                                    String fileTitle = "a" + UUID.randomUUID().toString() + "_" + hypermodel.getVersion() + "." + kind;
                                    this.storeWorkflowDescriptionForModel(model, fileTitle, kind, workflowDescription, token)
                                            .thenCompose(model1 -> storeParameters(model1, false, inputs, token))
                                            .thenCompose(model2 -> storeParameters(model2, true, outputs, token))
                                            .whenComplete((model3, throwable) -> {
                                                if (throwable == null) {
                                                    cacheModel(model3);
                                                }
                                                else {
                                                    log.info("storeHypermodel", throwable.getCause());
                                                }
                                            });
                                    return model;
                                }));
    }

    CompletableFuture<Model> storeHyperModelAsTool(Hypermodel hypermodel, SAMLToken token) {
        /*
        storeTool is a POST HTTP method used for storing a new model/tool (basic information).
        storeTool method accepts 7 input parameter which should all be passed through request body.
        Encoding: application/x-­‐www-­‐form-­‐urlencoded. These input parameters are the following:

          title -> Required. (title of model/tool)
          version -> Required. (version of model/tool. Should be in the format X.X for example 1.2)
          extra_parameters -> Not Required. (A string of flag-value pairs that should be included in command line)
          executable_path -> Not Required. (The relative path of the executable inside zip folder)
          strongly_coupled -> Required. (Is a strongly coupled model? 1 for true and 0 for false)
          description -> Not required. (description of model/tool)
          comment -> Not required. (comments on model/tool)
          semtype -> Not required. (url representing semantic information regarding this model/tool)

         The JSON object returned by method storeTool has one key, named id, and one value
         which is associated with this key.
         */

        // If the hypermodel has already a Repo Id do not store it as a new Model
        // We will Update it, instead
        if (hypermodel.getPublishedRepoId().isPresent())
            return CompletableFuture.completedFuture(hypermodel.toModel());

        Map<String, String> m = new HashMap<>();
        m.put("title", hypermodel.getName());
        m.put("version", hypermodel.versionStr());
        m.put("strongly_coupled", hypermodel.isStronglyCoupled() ? "1" : "0");
        m.put("description", hypermodel.getDescription());
        final String url = BASE_URI + "storeTool/";
//        final String url = "http://localhost:7676/" + "storeTool/";
        return this.apiServer.postForm(url, token, m)
                .thenApply(JSONObject.class::cast)
                .thenApply(jsonObject -> {
                    final RepositoryId repoId = RepositoryId.fromJsonObj(jsonObject, "id");
                    log.info("MODEL {} stored in Repo as {}", hypermodel.getName(), repoId);
                    final UUID uuid = Optional.ofNullable(jsonObject.getAsString("uuid"))
                            .map(UUID::fromString)
                            .orElseGet(UUID::randomUUID);
                    return hypermodel.withRepoId(repoId).toModel();
                });

    }

    CompletableFuture<Model> storeWorkflowDescriptionForModel(Model model,
                                                              String title,
                                                              WorkflowKind kind,
                                                              String workflowDescription,
                                                              SAMLToken token) {
        /*
        storeFile is a POST HTTP method used for storing a new file (along with its metadata).
        This file should be always associated with a model/tool.
        storeFile method accepts 9 input parameter which should all be passed through request body.
        Encoding: Multipart/form-data. These input parameters are the following:
            file -> Required. (The actual file (blob))
            tool_id -> Required. (link to the model/tool)
            title -> Required. (title of the file.)
            description -> Not required. (description of the file)
            kind -> Not required. (type of the file (document, source code, binary, etc.)
            license -> Not required. (license regarding this file)
            sha1sum -> Not required. (sha1sum of this file)
            comment -> Not required. (comments on this file)
            engine -> Not required. (engine suitable for running this file)
         The JSON object returned by method storeFile has one key, named id,
         and one value which is associated with this key.
         */

        final String url = BASE_URI + "storeFile/";
//        final String url = "http://localhost:7676/" + "storeFile/";
        Map<String, String> m = new HashMap<>();
//        m.put("file", workflowDescription);
        final String toolId = model.getId().toString();
        m.put("tool_id", toolId);
        m.put("title", title);
        m.put("kind", kind.toFileKind());
        m.put("description", "Workflow description for this hypermodel");
        m.put("comment", "Uploaded by HME©");
//        return this.apiServer.postFormMultipart(url, token, m)

        Map<String, ByteBuffer> data = new HashMap<>();
        data.put("file", ByteBuffer.wrap(workflowDescription.getBytes(Charset.defaultCharset())));
        return this.apiServer.postForm(url, token, m, data)
                .thenApply(JSONObject.class::cast)
                .thenApply(jsonObject -> jsonObject.getAsString("id"))
                .thenApply(id -> model);

    }
    private CompletableFuture<Model> storeParameters(Model model,
                                                     boolean isOutput,
                                                     List<ModelParameter> params,
                                                     SAMLToken token) {
        /*
        storeParameter is a POST HTTP method used for storing the parameter information
        of a given model/tool. storeParameter method accepts 13 input parameters which
         should all be passed through request body.
          Encoding: application/x-­‐www-­‐form-­‐urlencoded. These input parameters are the following:
           tool_id -> Required. (link to the model/tool)
           name -> Required. (name of the parameter)
           description -> Not required. (description of the parameter)
           data_type -> Required. (data type of the parameter)
           unit -> Not Required. (unit used in this parameter)
           flag -> Not Required. (flag used by hypomodel to read this parameter. Applicable only to static input parameters)
           data_range -> Required. (data range of this parameter)
           is_mandatory -> Required. (1 if mandatory, 0 otherwise)
           is_output -> Required. (1 if output, 0 otherwise)
           comment -> Not Required. (comments on this parameter)
           default_value -> Required. (default value of this parameter)
           is_static -> Required. (1 if static, 0 otherwise)
           semtype -> Not Required. (url representing semantic information regarding this parameter)
         The JSON object returned by method storeParameter has one key, named id, and one value which is associated with this key.
         */

          final String url = BASE_URI + "storeParameter/";

        final List<CompletableFuture<ModelParameter>> futures = params.stream().map(p -> {
            Map<String, String> m = new HashMap<>();
            final String toolId = model.getId().toString();
            m.put("tool_id", toolId);
            m.put("name", p.getName());
            m.put("description", p.getDescription());
            m.put("data_type", p.getDataType());
            m.put("unit", p.getUnit());
            m.put("data_range", p.getRange());
            m.put("is_mandatory", p.isMandatory() ? "1" : "0");
            m.put("is_output", isOutput ? "1" : "0");
            m.put("default_value", p.getDefaultValue().orElse(""));

            final String semtypes = p.getSemTypes().stream().collect(Collectors.joining(" "));
            m.put("semtype", semtypes);
            m.put("is_static", p.isDynamic() ? "0" : "1");

            return this.apiServer.postForm(url, token, m)
                    .thenApply(JSONObject.class::cast)
                    .thenApply(jsonObject -> RepositoryId.fromJsonObj(jsonObject,"id"))
                    .thenApply(p::withId)
                    .whenComplete(((modelParameter, throwable) -> {
                        if (throwable != null)
                            log.info("Sending storeParam failed, body:{}", m);
                    }));
        }).collect(Collectors.toList());
        return FutureUtils.sequence(futures)
                .thenApply(updatedParams -> {
                    if (isOutput)
                        model.setOutputs(updatedParams);
                    else
                        model.setInputs(updatedParams);
                    return model;
                });
    }

    @Override
    public void close() throws Exception {
        this.apiServer.close();
    }

}
