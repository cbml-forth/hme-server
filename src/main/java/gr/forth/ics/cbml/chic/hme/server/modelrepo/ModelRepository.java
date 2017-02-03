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
import gr.forth.ics.cbml.chic.hme.server.WebApiServer;
import gr.forth.ics.cbml.chic.hme.server.utils.FutureUtils;
import lombok.Value;
import lombok.extern.java.Log;
import lombok.val;
import net.minidev.json.JSONObject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by ssfak on 28/12/15.
 */
@Log
public class ModelRepository implements AutoCloseable {
    public final static String AUDIENCE = "https://mr.chic-vph.eu/";
    final static String BASE_URI = AUDIENCE + "model_app/";
    final WebApiServer apiServer;

    public ModelRepository(int concurrency) {
        this.apiServer = new WebApiServer(concurrency);
    }

    public WebApiServer apiServer() {
        return this.apiServer;
    }

    enum WorkflowKind {
        T2FLOW, XMML, OTHER;

        public static WorkflowKind fromString(String text) {
            if ("t2flow".equalsIgnoreCase(text))
                return T2FLOW;
            if ("xmml".equalsIgnoreCase(text))
                return XMML;
            return OTHER;
        }

        public String toFileKind() {
            if (this == T2FLOW)
                return "t2flow";
            if (this == XMML)
                return "xmml";
            return "other";
        }
    }
    enum Model_Cache {

        INSTANCE;


        private final ConcurrentHashMap<String, Model> cache_;

        Model_Cache() {
            this.cache_ = new ConcurrentHashMap<>();
        }

        public boolean contains(final String modelId) {

            final boolean b = this.cache_.containsKey(modelId);
            return b;
        }

        public Model get(final String modelId) {
            return this.cache_.get(modelId);
        }

        public void put(final String modelId, Model m) {
            this.cache_.put(modelId, m);
        }

        public void clear() {
            this.cache_.clear();
        }
    }
    private CompletableFuture<JSONObject> getModelParamsJSON(final String modelId, final SAMLToken token) {
        final String url = BASE_URI + "getParametersByToolId/";
        return this.apiServer.getJsonAsync(url, token, Collections.singletonMap("tool_id", modelId))
                .thenApply(JSONObject.class::cast);
    }

    private CompletableFuture<JSONObject> getModelJSON(final String modelId, final SAMLToken token) {
        final String url = BASE_URI + "getToolById/";
        return this.apiServer.getJsonAsync(url, token, Collections.singletonMap("id", modelId))
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

    public CompletableFuture<List<File>> getModelFilesJSON(final String modelId,
                                                           final SAMLToken token) {
        final String url = BASE_URI + "getFilesByToolId/";
        HashMap<String, String> qs = new HashMap<>();
        qs.put("tool_id", modelId);
        return this.apiServer.getJsonAsync(url, token, qs)
                .thenApplyAsync(jsonAware -> ((JSONObject) jsonAware).values()
                        .stream()
                        .map(JSONObject.class::cast)
                        .map(File::fromJSON)
                        .collect(Collectors.toList()));
    }

    public CompletableFuture<Optional<File>> getWorkflowFileForModel(final String modelId,
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
                .thenApplyAsync(js -> {
                    List<Input> inputs = new ArrayList<>();
                    List<Output> outputs = new ArrayList<>();

                    js.values().forEach(o -> {
                        JSONObject jsonObject = (JSONObject) o;
                        final boolean isOutput = jsonObject.getAsNumber("is_output").intValue() == 1;
                        if (isOutput)
                            outputs.add(Output.fromJson(jsonObject));
                        else
                            inputs.add(Input.fromJson(jsonObject));
                    });
                    inputs.sort(Comparator.comparing(Input::getId));
                    outputs.sort(Comparator.comparing(Output::getId));

                    model.setInputs(inputs);
                    model.setOutputs(outputs);
                    return model;
                })
                .whenComplete((m, ex) -> {
                    if (m != null)
                        cacheModel(m);
                });
    }


    public CompletableFuture<Model> getModel(final String modelId,
                                             final SAMLToken token)
    {

        if (Model_Cache.INSTANCE.contains(modelId))
            return CompletableFuture.completedFuture(Model_Cache.INSTANCE.get(modelId));

        final CompletableFuture<JSONObject> paramsJSONFut = getModelParamsJSON(modelId, token);
        final CompletableFuture<JSONObject> modelJSONFut = getModelJSON(modelId, token);
        return modelJSONFut.thenCombineAsync(paramsJSONFut,
                (modelJson, paramsJson) -> {
                    final Model model = parseModelJson(modelJson);

                    model.setInputs(this.getInputsFromJSON(paramsJson));
                    model.setOutputs(this.getOutputsFromJSON(paramsJson));

                    Model_Cache.INSTANCE.put(modelId, model);
                    return model;
                });
    }

    private static Model parseModelJson(JSONObject modelJson) {
        final String modelId = modelJson.getAsString("id");
        final String name = modelJson.getAsString("title");
        final String description = modelJson.getAsString("description");
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
        log.info("Caching model " +  m.getId());
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
                                        String modelId = js.getAsString("id");
                                        boolean isCached = Model_Cache.INSTANCE.contains(modelId);
                                        if (isCached) {
                                            final Model model = Model_Cache.INSTANCE.get(modelId);
                                            return CompletableFuture.completedFuture(model);
                                        } else {
                                            final Model model = parseModelJson(js);
                                            return this.fillModelParams(model, token);
                                        }
                                    })
                                    .collect(Collectors.toList());
                    return FutureUtils.sequence(futureList);
                });
    }


    private List<Input> getInputsFromJSON(final JSONObject js) {
        return js.values().stream()
                .map(obj -> (JSONObject) obj)
                .filter(obj -> obj.getAsNumber("is_output").intValue() == 0)
                .map(Input::fromJson)
                .collect(Collectors.toList());
    }



    private List<Output> getOutputsFromJSON(final JSONObject js) {
        return js.values().stream()
                .map(obj -> (JSONObject) obj)
                .filter(obj -> obj.getAsNumber("is_output").intValue() == 1)
                .map(Output::fromJson)
                .collect(Collectors.toList());
    }


    public CompletableFuture<String> storeModelAndWorkflow(String title, String description, String version,
                                                           boolean isStronglyCoupled,
                                                           WorkflowKind kind,
                                                           String workflowDescription,
                                                           SAMLToken token) {
        return this.storeModel(title, description, version, isStronglyCoupled, token)
                .thenCompose(modelId -> {
                    String fileTitle = String.valueOf((title + ":" + version + "." + kind).hashCode());
                    return this.storeWorkflowDescriptionForModel(modelId, fileTitle, kind, workflowDescription, token)
                            .thenApply(fileId -> modelId);
                });
    }

    private CompletableFuture<String> storeModel(String title, String description, String version,
                                                 boolean isStronglyCoupled, SAMLToken token) {
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
        Map<String, String> m = new HashMap<String, String>() {{
            put("title", title);
            put("version", version);
            put("strongly_coupled", isStronglyCoupled ? "1" : "0");
            put("description", description);
        }} ;
        final String url = BASE_URI + "storeTool/";
        return this.apiServer.postForm(url, token, m)
                .thenApply(JSONObject.class::cast)
                .thenApply(jsonObject -> jsonObject.getAsString("id"));

    }

    private CompletableFuture<String> storeWorkflowDescriptionForModel(String modelId, String title,
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
        Map<String, String> m = new HashMap<String, String>() {{
            put("file", workflowDescription);
            put("tool_id", modelId);
            put("title", title);
            put("kind", kind.toFileKind());
            put("description", "Workflow description for this hypermodel");
            put("comment", "Uploaded by HME");
        }};
        return this.apiServer.postFormMultipart(url, token, m)
                .thenApply(JSONObject.class::cast)
                .thenApply(jsonObject -> jsonObject.getAsString("id"));

    }

    @Override
    public void close() throws Exception {
        this.apiServer.close();
    }

}
