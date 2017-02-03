package gr.forth.ics.cbml.chic.hme.server.execution;

import gr.forth.ics.cbml.chic.hme.server.SAMLToken;
import gr.forth.ics.cbml.chic.hme.server.WebApiServer;
import gr.forth.ics.cbml.chic.hme.server.modelrepo.Input;
import lombok.Value;
import lombok.experimental.Wither;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Created by ssfak on 31/1/17.
 */
@Slf4j
public class ExecutionFramework implements AutoCloseable {
    //    final static String API_BASE_URI = "http://localhost:3456/";
    public final static String AUDIENCE = "https://hf.chic-vph.eu";
    public final static String API_BASE_URI = AUDIENCE + "/api";

    final WebApiServer apiServer;


    public WebApiServer apiServer() {
        return this.apiServer;
    }

    public ExecutionFramework(int concurrency) {
        this.apiServer = new WebApiServer(concurrency);
    }

    public CompletableFuture<List<String>> getWorkflowList(final SAMLToken token) {

        return this.apiServer.getJsonAsync(API_BASE_URI + "/director/workflowlist/", token)
                .thenApply(JSONArray.class::cast)
                .thenApply(jsArray -> jsArray.stream()
                        .map(JSONObject.class::cast)
                        .map(j -> j.getAsString("id"))
                        .collect(Collectors.toList()));
    }


    public String createInputFile(final List<Input> inputs) {

        String b = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<b:dataThingMap xmlns:b=\"http://org.embl.ebi.escience/baclava/0.1alpha\" xmlns:s=\"http://org.embl.ebi.escience/xscufl/0.1alpha\">\n";

        Base64.Encoder encoder = Base64.getEncoder();

        final Charset utf8 = StandardCharsets.UTF_8;
        for (Input input : inputs) {
            final byte[] bytes = input.getValue().orElse("").getBytes(utf8);
            String in =
                    "  <b:dataThing key=\"" + input.getName() + "\">\n" +
                            "    <b:myGridDataDocument lsid=\"\" syntactictype=\"'text/plain'\">\n" +
                            "      <s:metadata>\n" +
                            "        <s:mimeTypes>\n" +
                            "          <s:mimeType>text/plain</s:mimeType>\n" +
                            "        </s:mimeTypes>\n" +
                            "      </s:metadata>\n" +
                            "      <b:dataElement lsid=\"\">\n" +
                            "        <b:dataElementData>" + encoder.encodeToString(bytes) + "</b:dataElementData>\n" +
                            "      </b:dataElement>\n" +
                            "    </b:myGridDataDocument>\n" +
                            "  </b:dataThing>\n";
            b += in;
        }
        b += "</b:dataThingMap>";
        return b;
    }

    public CompletableFuture<String> getWorkflowStatus(final String workflowId, final SAMLToken token) {
        return this.apiServer
                .getJsonAsync(API_BASE_URI + "/director/workflowlist/" + workflowId + "/status/", token)
                .thenApply(jsonAware -> {
                    final JSONObject jsonObject = (JSONObject) jsonAware;
                    return jsonObject.getAsString("workflow_status");
                });

    }


    @Value
    public static class WorkflowStatus {
        @Wither String status;
        String workflowId;
        UUID workflowUuid;
    }

    public CompletableFuture<WorkflowStatus> startWorkflow(final WorkflowStatus w, final SAMLToken token) {
        log.info("STARTING WORKFLOW : {}", w.getWorkflowId());
        return this.apiServer
                .postFormMultipart(API_BASE_URI + "/director/workflowlist/" + w.getWorkflowId() + "/status/", token,
                        Collections.singletonMap("workflow_status", "Operating"), false)
                .thenApply(JSONObject.class::cast)
                .thenApply(jsonObject -> {
                    String status = jsonObject.getAsString("workflow_status");
                    return w.withStatus(status);
                });
    }


    @Override
    public void close() throws Exception {
        this.apiServer.close();
    }

    public CompletableFuture<WorkflowStatus> submitWorkflow(final Map<String, String> inputParams, final SAMLToken token) {
        return this.apiServer
                .postForm(API_BASE_URI + "/director/workflowlist/", token, inputParams)
                .thenApply(JSONObject.class::cast)
                .handle((js, ex) -> {
                    if (ex != null) {
                        ex.printStackTrace();
                        return null;

                    }
                    UUID workflow_uuid = UUID.fromString(js.getAsString("workflow_id"));
                    String workflow_status = js.getAsString("workflow_status");
                    String workflow_id = js.getAsString("id");
                    return new WorkflowStatus(workflow_status, workflow_id, workflow_uuid);
                });
    }
}

