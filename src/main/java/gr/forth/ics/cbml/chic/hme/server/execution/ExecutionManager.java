package gr.forth.ics.cbml.chic.hme.server.execution;

import gr.forth.ics.cbml.chic.hme.server.SAMLToken;
import gr.forth.ics.cbml.chic.hme.server.TokenManager;
import gr.forth.ics.cbml.chic.hme.server.modelrepo.*;
import gr.forth.ics.cbml.chic.hme.server.utils.FutureUtils;
import gr.forth.ics.cbml.chic.hme.server.utils.ZipUtils;
import lombok.extern.slf4j.Slf4j;
import nu.xom.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Created by ssfak on 31/1/17.
 */
@Slf4j
public class ExecutionManager {

    final TokenManager tokenManager;
    final ExperimentRepository expRepository;
    final ModelRepository modelRepository;
    final ExecutionFramework execFramework;

    final Path storageDir;

    public ExecutionManager(final Path storageDir, TokenManager tokenManager,
                     ExperimentRepository expRepository, ModelRepository modelRepository,
                     ExecutionFramework execFramework) {
        this.tokenManager = tokenManager;
        this.storageDir = storageDir;
        this.expRepository = expRepository;
        this.modelRepository = modelRepository;
        this.execFramework = execFramework;
    }

    public CompletableFuture<Experiment> runHypermodel(final RepositoryId hyperModelId,
                                                       final String experiment_description,
                                                       final String patient_pseudonym,
                                                       final List<ModelParameter> inputs,
                                                       // final DBManager db,
                                                       final String actAs) {
        final CompletableFuture<SAMLToken> expRepoTokenFut = tokenManager.getDelegationToken(expRepository.AUDIENCE, actAs);
        final CompletableFuture<SAMLToken> modelRepoTokenFut = tokenManager.getDelegationToken(modelRepository.AUDIENCE, actAs);
        final CompletableFuture<SAMLToken> execTokenFut = tokenManager.getDelegationToken(execFramework.AUDIENCE, actAs);
        return CompletableFuture.allOf(expRepoTokenFut, modelRepoTokenFut, execTokenFut)
                .thenCompose(__ -> {
                    final SAMLToken modelRepoToken = modelRepoTokenFut.join();
                    final SAMLToken expRepoToken = expRepoTokenFut.join();
                    final SAMLToken execToken = execTokenFut.join();
                    return runHypermodelImpl(hyperModelId, experiment_description, patient_pseudonym, inputs, modelRepoToken, expRepoToken, execToken);
                });

    }

    public CompletableFuture<List<ModelParameter>> getExecutionInputs(final UUID experimentId,
                                                             final String actAs) {
        final CompletableFuture<SAMLToken> expRepoTokenFut = this.tokenManager.getDelegationToken(expRepository.AUDIENCE, actAs);
        final CompletableFuture<SAMLToken> modelRepoTokenFut = this.tokenManager.getDelegationToken(modelRepository.AUDIENCE, actAs);
        return FutureUtils.thenComposeBoth(expRepoTokenFut, modelRepoTokenFut, (expRepoToken, modelRepoToken) ->
                this.getExecutionInputsImpl(experimentId, modelRepoToken, expRepoToken));
    }

    public CompletableFuture<Path> downloadResults(final UUID experimentUuid,
                                                   final List<Output> outputs,
                                                   final Path dir,
                                                   final String actAs) {
        return this.tokenManager.getDelegationToken(expRepository.AUDIENCE, actAs)
                .thenCompose(token -> this.downloadResultsImpl(experimentUuid, outputs, dir, token));

    }

    private CompletableFuture<Experiment> runHypermodelImpl(RepositoryId hyperModelId,
                                                            String experiment_description,
                                                            String patient_pseudonym,
                                                            List<ModelParameter> inputs,
                                                            SAMLToken modelRepoToken,
                                                            SAMLToken expRepoToken,
                                                            SAMLToken execToken) {
        // We need to:
        // 1) Create a new experiment in the inSilico Trial ("Experiment") Repository
        // 2) Launch the execution -- contact the Execution Framework

        //final Map<String, String> params = inputs.stream().collect(toMap(Input::getName, Input::getValue));
        return this.expRepository.createNewExperiment(hyperModelId, experiment_description, patient_pseudonym, expRepoToken)
                // .thenCompose(experiment -> db.insertExperimentToDb(experiment, params, params))
                .thenCompose(experiment -> this.submitWorkflow(hyperModelId, experiment, inputs,
                        modelRepoToken, expRepoToken, execToken));
    }
    private CompletableFuture<Experiment> submitWorkflow(final RepositoryId hyperModelId,
                                                         final Experiment experiment,
                                                         final List<ModelParameter> inputs,
                                                         //final DBManager db,
                                                         final SAMLToken modelRepoToken,
                                                         final SAMLToken expRepoToken,
                                                         final SAMLToken execToken) {

        System.err.println("SUBMITTING workflow for EXPERIMENT :" + experiment.id);


        // 1. Find and retrieve the t2Flow or xMML file Id associated with the given hypermodel:
        final CompletableFuture<String> t2FlowFileFut = modelRepository.getModelFilesJSON(hyperModelId, modelRepoToken)
                .thenApply(list -> {
                    for (ModelRepository.File f : list) {
                        final String kind = f.getKind();
                        if ("t2flow".equalsIgnoreCase(kind) || "xmml".equalsIgnoreCase(kind))
                            return f.getId();
                    }
                    log.error("No workflow file (xmml or t2flow) found for {}", hyperModelId);
                    throw new IndexOutOfBoundsException("t2Flow (or xmml) file not found for model " + hyperModelId); //list.get(0).getId();
                });

        // 2. Upload the Inputs in 'Baclava' format to the trial repository:
        final String baclava = execFramework.createInputFile(inputs);
        log.info("BACLAVA: {}", baclava);
        final CompletableFuture<String> inputFileFut =
                expRepository.addFileToSubject(experiment.getSubjectIn().getId(), "inputs", "baclava",
                        ByteBuffer.wrap(baclava.getBytes(StandardCharsets.UTF_8)), expRepoToken);

        // 3. Submit the "workflow" to the ExecutionFramework
        return FutureUtils.thenComposeBoth(inputFileFut, t2FlowFileFut,
                (inputFileId, t2FlowFileId) -> {
                    final RepositoryId experimentId = experiment.id;
                    final String subject_out_id = experiment.getSubjectOut().getId().toString();

                    HashMap<String, String> m = new HashMap<>();
                    m.put("workflow_title", experiment.description == null ? "Experiment" : experiment.description);
                    m.put("workflow_description", "Execution experiment: " + experimentId);
                    m.put("workflow_comment", "Invoked by HME©, all rights reserved :-)");
                    m.put("model_url", modelRepository.fileUrl(t2FlowFileId));
                    m.put("model_id", hyperModelId.getId()+"");
                    m.put("inputset_url", expRepository.fileUrl(inputFileId));
                    m.put("experiment_id", experimentId.toString());
                    m.put("experiment_uuid", experiment.uuid);
                    m.put("subject_out_id", subject_out_id);
                    log.info("Ready to send to VPH-HF Director... experiment id={} inputs={}",experimentId, m);
                    return execFramework.submitWorkflow(m, execToken);
                })
                // 4. Start the workflow
                .thenCompose(workflowStatus -> {
                    log.info("Starting workflow {} for exp {}", workflowStatus.getWorkflowId(), experiment.id);
                    return execFramework.startWorkflow(workflowStatus, execToken);
                })
                .thenApply(workflowStatus -> {
                    experiment.workflow_uuid = workflowStatus.getWorkflowUuid();
                    experiment.status = workflowStatus.getStatus();
                    log.info("Experiment: {}", experiment);
                    return experiment;
                });

               /*{
                    experiment.workflow_uuid = workflowStatus.workflow_uuid;
                    experiment.status = Experiment.EXP_RUN_STATE.RUNNING;
                    return execFramework.startWorkflow(workflowStatus, execToken).thenApply(__ -> experiment);
                })
                .thenCompose((Tuple2<Experiment, ExecutionFramework.WorkflowStatus> pair) ->
                        db.updateExperimentWorkflow(experiment.getUUID(), pair.v1().workflow_uuid)
                                .thenApply(__ -> pair))
                .thenCompose(pair ->
                        execFramework.startWorkflow(pair.v2(), execToken).thenApply(__ -> pair.v1()))*/

    }


    public Path directoryForExperiment(final UUID experimentUuid) {
        return directoryForExperiment(this.storageDir, experimentUuid.toString());
    }

    private static Path directoryForExperiment(final Path parent, final String experimentUuid) {
        final String f1 = experimentUuid.substring(0, 2);
        final String f2 = experimentUuid.substring(2, 4);
        final String rest = experimentUuid.substring(4);

        final Path path = Paths.get(f1, f2, rest);

        return parent.resolve(path);

    }


    private CompletableFuture<List<ModelParameter>> getExecutionInputsImpl(final UUID experimentId,
                                                                           final SAMLToken modelRepoToken,
                                                                           final SAMLToken expRepoToken) {
        final CompletableFuture<Experiment> expFut = expRepository.getExperiment(expRepoToken, experimentId);
        final CompletableFuture<ByteBuffer> byteBufferFut = expFut.thenCompose(experiment -> expRepository.getExperimentInputs(experiment, expRepoToken));
        final CompletableFuture<Model> modelFut = expFut.thenCompose(experiment -> modelRepository.getModel(experiment.trial.model_id, modelRepoToken));

        return byteBufferFut.thenCombine(modelFut, (buffer, model) -> {
            final Base64.Decoder decoder = Base64.getDecoder();
            try {
                Element root = new Builder().build(new ByteArrayInputStream(buffer.array())).getRootElement();
                final XPathContext xPathContext = new XPathContext("b", "http://org.embl.ebi.escience/baclava/0.1alpha");
                final Nodes nodes = root.query("b:dataThing", xPathContext);
                Map<String, String> hm = new HashMap<>();
                for (int i = 0; i < nodes.size(); i++) {
                    final Element node = (Element) nodes.get(i);
                    final String name = node.getAttributeValue("key");
                    final Node data = node.query(".//b:dataElementData", xPathContext).get(0);
                    final String value = new String(decoder.decode(data.getValue()), StandardCharsets.UTF_8);
                    hm.put(name, value);
                }
                return model.getInputs().stream().map(in -> {
                    if (hm.containsKey(in.getName()))
                        return in.withValue(hm.get(in.getName()));
                    return in;
                }).collect(Collectors.toList());

            } catch (ParsingException e) {
                e.printStackTrace();
                throw new RuntimeException(e);

            } catch (IOException e) {
                e.printStackTrace();
                throw new UncheckedIOException(e);
            }
        });
    }


    private CompletableFuture<RepositoryId> outputsFileIdForExperiment(final UUID experimentUuid, SAMLToken token)
    {

        return expRepository.getExperiment(token, experimentUuid)
                .thenApply((Experiment experiment) -> {
                    if (experiment.status == Experiment.EXP_RUN_STATE.FINISHED_OK && experiment.getOutputFiles().size() > 0) {
                        final Optional<TrFile> file = experiment.getOutputFiles().stream()
                                .filter(TrFile::isOutput)
                                .findFirst(); // XXX: can it have more than one (zip) output files?
                        if (!file.isPresent())
                            throw new RuntimeException("Experiment does not have outputs");

                        final RepositoryId fileId = file.get().getId();
                        return fileId;
                    }
                    throw new RuntimeException("Experiment has not finished?");
                });
    }

    private CompletableFuture<Path> downloadResultsImpl(final UUID experimentUuid,
                                                        final List<Output> outputs,
                                                        final Path dir,
                                                        SAMLToken token) {
        return outputsFileIdForExperiment(experimentUuid, token)
                .thenCompose(fileId -> {
                    try {
                        if (!dir.toFile().exists()) {
                            Files.createDirectories(dir);
                            System.err.println("* Outputs of experiment " + experimentUuid + " will be saved at " + dir);
                        }
                        return downloadAndUnzip(dir, fileId, outputs, token);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return FutureUtils.completeExFuture(e);
                    }
                });
    }

    private CompletableFuture<Path> downloadAndUnzip(final Path dir,
                                                     final RepositoryId file_id,
                                                     final List<Output> outputs,
                                                     SAMLToken token) throws IOException {
        final Path tempFile = Files.createTempFile(dir, "output-", ".zip");
        final OutputStream outs = Files.newOutputStream(tempFile);

        // System.err.println("Downloading file id="+file_id + " into " + dir);
        return expRepository.downloadFile(file_id, outs, token)
                .thenApplyAsync(aVoid -> {
                    try {
                        if (ZipUtils.isZipFile(tempFile)) {
                            ZipUtils.unzipFile(dir, tempFile.toFile());
                            Files.delete(tempFile);
                        }
                        for (Output output : outputs) {

                            final Path source = dir.resolve(output.getName());
                            final String outputCanName = output.getDefaultValue();
                            if (!source.toFile().exists()) {
                                System.err.println("Output '" + output.getName() + "' not found in result zip");
                                continue;
                            }
                            final Path fileName = (outputCanName == null || "".equals(outputCanName)) ?
                                    Paths.get(output.getName()) :
                                    Paths.get(outputCanName).getFileName();
                            final Path target = dir.resolve(fileName);
                            if (!target.getParent().toFile().exists())
                                Files.createDirectories(target.getParent());
                            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new UncheckedIOException(e);
                    }
                    return dir;
                });
    }

}
