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
import net.minidev.json.JSONObject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by ssfak on 28/12/15.
 */
public class ModelRepository implements AutoCloseable {
    public final static String AUDIENCE = "https://mr.chic-vph.eu/";
    final static String BASE_URI = AUDIENCE + "model_app/";
    final WebApiServer apiServer;

    public ModelRepository(int concurrency, boolean useCache) {
        this.apiServer = new WebApiServer(concurrency);
    }

    public WebApiServer apiServer() {
        return this.apiServer;
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


    public static class File {
        String id;
        String sha;
        String description;
        String kind;


        public static File fromJSON(final JSONObject jsonFile) {
            final File f = new File();
            f.id = jsonFile.getAsString("id");
            f.sha = jsonFile.getAsString("sha1sum");
            f.description = jsonFile.getAsString("description");
            f.kind = jsonFile.getAsString("kind");
            return f;
        }

        public String getId() {
            return id;
        }

        public String getDescription() {
            return description;
        }

        public String getKind() {
            return kind;
        }

        public String getSha() {
            return sha;
        }


    }

    public void clearCache()
    {
        Model_Cache.INSTANCE.clear();
    }
    public CompletableFuture<List<File>> getModelFilesJSON(final String modelId, final SAMLToken token) {
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

    public CompletableFuture<File> getWorkflowFileForModel(final String modelId, final SAMLToken token) {
        return this.getModelFilesJSON(modelId, token)
                .thenApply(list -> {
                    for (File f: list) {
                        if (f.kind == "t2flow")
                            return f;
                    }
                    return null;
                });
    }

    public static String fileUrl(final String file_id) {
        return BASE_URI + "getFileById/?id=" + file_id;
    }

    public CompletableFuture<java.io.File> downloadFile(final String fileId, final SAMLToken token) throws IOException {
        final String url = BASE_URI + "getFileById/";
        HashMap<String, String> qs = new HashMap<>();
        qs.put("id", fileId);


        final Path tempFile = Files.createTempFile("", ".data");
        final FileOutputStream outputStream = new FileOutputStream(tempFile.toFile());

        CompletableFuture<java.io.File> fut = new CompletableFuture<>();
        this.apiServer.downloadContent(url, token, qs, outputStream)
                .whenComplete((v, ex)->{
                    if (ex != null)
                        fut.completeExceptionally(ex);
                    else
                        fut.complete(tempFile.toFile());
                });
        return fut;
    }


    public CompletableFuture<List<Input>> getModelInputs(final String modelId, final SAMLToken token) {
        final CompletableFuture<JSONObject> jsonAsync = getModelParamsJSON(modelId, token);

        return jsonAsync.thenApplyAsync(this::getInputsFromJSON);
    }


    public CompletableFuture<List<Output>> getModelOutputs(final String modelId, final SAMLToken token) {
        final CompletableFuture<JSONObject> jsonAsync = getModelParamsJSON(modelId, token);

        return jsonAsync.thenApplyAsync(this::getOutputsFromJSON);
    }

    public CompletableFuture<Model> getModel(final String modelId, final SAMLToken token) {

        if (Model_Cache.INSTANCE.contains(modelId))
            return CompletableFuture.completedFuture(Model_Cache.INSTANCE.get(modelId));

        final CompletableFuture<JSONObject> paramsJSONFut = getModelParamsJSON(modelId, token);
        final CompletableFuture<JSONObject> modelJSONFut = getModelJSON(modelId, token);
        return modelJSONFut.thenCombineAsync(paramsJSONFut,
                (modelJson, paramsJson) -> {
                    final String name = modelJson.getAsString("title");
                    final String description = modelJson.getAsString("description");
                    final String uuid = modelJson.getAsString("uuid");
                    final Model model = new Model(modelId, name, description, uuid);

                    model.setInputs(this.getInputsFromJSON(paramsJson));
                    model.setOutputs(this.getOutputsFromJSON(paramsJson));

                    Model_Cache.INSTANCE.put(modelId, model);
                    return model;
                });
    }


    public CompletableFuture<List<Model>> getAllModels(final SAMLToken token) {
        final String url = BASE_URI + "getAllTools/";
        return this.apiServer.getJsonAsync(url, token)
                .thenApply(JSONObject.class::cast)
                .thenCompose(jsonObject -> {
                    final List<CompletableFuture<Model>> futureList = jsonObject.values().stream()
                            .map(JSONObject.class::cast)
                            .map(js -> {
                                String modelId = js.getAsString("id");
                                return this.getModel(modelId, token);
                            })
                            .collect(Collectors.toList());
                    return FutureUtils.sequence(futureList);
                });
    }


    private List<Input> getInputsFromJSON(final JSONObject js) {
        return js.values().stream()
                .map(obj -> (JSONObject) obj)
                .filter(obj -> obj.getAsNumber("is_output").intValue() == 0)
                .map(jsonObject -> {
                    Input in = new Input();
                    in.setDataType(jsonObject.getAsString("data_type"));
                    in.setDefaultValue(jsonObject.getAsString("default_value"));
                    in.setDescription(jsonObject.getAsString("description").replace("\r", "").replace("\n", " "));

                    final List<String> semTypes = Arrays.asList(jsonObject.getAsString("semtype").split("\\s+"));
                    in.setSemTypes(semTypes);
                    in.setDynamic(jsonObject.getAsNumber("is_static").intValue() == 0);

                    in.setMandatory(jsonObject.getAsNumber("is_mandatory").intValue() == 1);
                    in.setName(jsonObject.getAsString("name"));
                    in.setRange(jsonObject.getAsString("data_range"));
                    in.setUnit(jsonObject.getAsString("unit"));
                    return in;
                })
                .collect(Collectors.toList());
    }


    private List<Output> getOutputsFromJSON(final JSONObject js) {
        return js.values().stream()
                .map(obj -> (JSONObject) obj)
                .filter(obj -> obj.getAsNumber("is_output").intValue() == 1)
                .map(jsonObject -> {
                    Output out = new Output();
                    out.setDataType(jsonObject.getAsString("data_type"));
                    out.setDefaultValue(jsonObject.getAsString("default_value"));
                    out.setDescription(jsonObject.getAsString("description"));
                    out.setDynamic(jsonObject.getAsNumber("is_static").intValue() == 0);
                    out.setName(jsonObject.getAsString("name"));
                    out.setUnit(jsonObject.getAsString("unit"));
                    return out;
                })
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
        this.apiServer.close();
    }

}
