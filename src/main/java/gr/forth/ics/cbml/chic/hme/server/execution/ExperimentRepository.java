package gr.forth.ics.cbml.chic.hme.server.execution;

/**
 * Created by ssfak on 31/1/17.
 */

import gr.forth.ics.cbml.chic.hme.server.SAMLToken;
import gr.forth.ics.cbml.chic.hme.server.WebApiServer;
import gr.forth.ics.cbml.chic.hme.server.modelrepo.RepositoryId;
import gr.forth.ics.cbml.chic.hme.server.utils.FutureUtils;
import gr.forth.ics.cbml.chic.hme.server.utils.UriUtils;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class is the client side representation of the "inSilico Trial Repository" that is used for
 * storing Experiments, i.e. runs of the hypermodels with their inputs and results...
 */
@Slf4j
public class ExperimentRepository implements AutoCloseable {

    public final URI AUDIENCE;
    final URI API_BASE_URI;
    final WebApiServer apiServer;

    public ExperimentRepository(URI apiUrl, int concurrency) {
        this.apiServer = new WebApiServer(concurrency);

        this.API_BASE_URI = UriUtils.baseURI(apiUrl);
        this.AUDIENCE = UriUtils.audienceURI(apiUrl);
    }

    public WebApiServer apiServer() { return this.apiServer; }

    @Override
    public void close() throws Exception {
        this.apiServer.close();
    }


    private ConcurrentHashMap<RepositoryId, Trial> trialCache_ = new ConcurrentHashMap<>();

    private CompletableFuture<Trial> getTrialByModelId(final RepositoryId modelId, final SAMLToken token) {

        return this.apiServer.getJsonAsync(API_BASE_URI + "getTrialByModelId/", token,
                Collections.singletonMap("id", modelId.getId()+""))
                .thenApply(jsonAware -> {
                    JSONObject jsonObject = (JSONObject) jsonAware;
                    if (!jsonObject.containsKey("id")) { // not found
                        return null;
                    }

                    final Trial trial = new Trial(RepositoryId.fromJsonObj(jsonObject, "id"), RepositoryId.fromJsonObj(jsonObject,"model_id"));
                    ExperimentRepository.this.trialCache_.put(trial.id, trial);
                    return trial;
                });
    }


    public CompletableFuture<Trial> getTrialById(final SAMLToken token, final RepositoryId trial_id) {
        if (trialCache_.containsKey(trial_id))
            return CompletableFuture.completedFuture(trialCache_.get(trial_id));

        return this.apiServer.getJsonAsync(API_BASE_URI + "getTrialById/", token,
                Collections.singletonMap("id", trial_id.getId()+""))
                .thenApply(jsonAware -> {
                    JSONObject jsonObject = (JSONObject) jsonAware;
                    if (jsonObject.isEmpty()) { // not found
                        return null;
                    }

                    final Trial trial = new Trial(trial_id, RepositoryId.fromJsonObj(jsonObject, "model_id"));
                    trial.setDescription(jsonObject.getAsString("description"));
                    ExperimentRepository.this.trialCache_.put(trial.id, trial);
                    return trial;
                });
    }

    public CompletableFuture<Trial> createNewOrReturnExistingTrial(final RepositoryId modelId,
                                                                   final String description,
                                                                   final SAMLToken token) {

        return this.getTrialByModelId(modelId, token)
                .thenCompose(trial -> {
                    if (trial == null) {
                        HashMap<String, String> form = new HashMap<>();
                        form.put("model_id", modelId.getId()+"");
                        form.put("description", description);

                        return this.apiServer.postForm(API_BASE_URI + "storeTrial/", token, form)
                                .thenApply(JSONObject.class::cast)
                                .thenApply(jsonObject -> new Trial(RepositoryId.fromJsonObj(jsonObject,"id"), modelId));
                    }
                    return CompletableFuture.completedFuture(trial);
                });

    }

    public CompletableFuture<Subject> createNewSubject(final String description,
                                                       final String subject_external_id,
                                                       final SAMLToken token) {
        HashMap<String, String> form = new HashMap<>();
        form.put("description", description);
        form.put("subject_external_id", subject_external_id);

        return this.apiServer.postForm(API_BASE_URI + "storeSubject/", token, form)
                .thenApply(JSONObject.class::cast)
                .thenApply(jsonObject -> {
                    final Subject subject = new Subject(RepositoryId.fromJsonObj(jsonObject,"id"), description);
                    subject.setExternalId(subject_external_id);
                    return subject;
                });

    }

    public CompletableFuture<String> addFileToSubject(final RepositoryId subject_id, final String title,
                                                      final String kind, final ByteBuffer b,
                                                      final SAMLToken token) {
        HashMap<String, String> form = new HashMap<>();
        form.put("subject_id", subject_id.toString());
        form.put("title", title);
        form.put("kind", kind);
        form.put("version", "1.0");

        return this.apiServer.postForm(API_BASE_URI + "storeTrFile/", token, form,
                Collections.singletonMap("file", b))
                .thenApply(jsonObject -> ((JSONObject) jsonObject).getAsString("id"));
    }

    public String fileUrl(final String file_id) {
        return API_BASE_URI.resolve("getTrFileById/?id=" + file_id).toString();
    }

    public CompletableFuture<Void> downloadFile(final RepositoryId file_id,
                                                final OutputStream whereToSave,
                                                final SAMLToken token) {

        return this.apiServer.downloadContent(API_BASE_URI + "getTrFileById/", token,
                Collections.singletonMap("id", file_id.toString()),
                whereToSave);
    }

    public CompletableFuture<Subject> getSubject(final SAMLToken token, final RepositoryId subjectId) {
        final CompletableFuture<List<TrFile>> fileIdsFut = getFilesOfSubject(token, subjectId);
        return this.apiServer.getJsonAsync(API_BASE_URI + "getSubjectById/", token, Collections.singletonMap("id", ""+subjectId.getId()))
                .thenApply(JSONObject.class::cast)
                .thenApply(jsonObject -> {
                    final Subject subject = new Subject(subjectId, jsonObject.getAsString("description"));
                    subject.setExternalId(jsonObject.getAsString("subject_external_id"));
                    return subject;
                })
                .thenCombine(fileIdsFut, (subject, fileIds) -> {
                    subject.setFiles(fileIds);
                    return subject;
                });
    }


    public CompletableFuture<List<TrFile>> getFilesOfSubject(final SAMLToken token, RepositoryId subjectId) {
        return this.apiServer.getJsonAsync(API_BASE_URI + "getTrFilesBySubjectId/", token, Collections.singletonMap("id", ""+subjectId.getId()))
                .thenApply(js -> ((JSONObject) js).values().stream()
                        .map(JSONObject.class::cast)
                        .map(v -> new TrFile(RepositoryId.fromJsonObj(v, "id"), v.getAsString("kind"),
                                DatatypeConverter.parseDateTime(v.getAsString("created_on")).toInstant()))
                        .sorted(Comparator.comparing(TrFile::getCreatedOn).reversed())
                        .collect(Collectors.toList()));
    }


    private CompletableFuture<Experiment> createNewExperimentImpl(final Trial trial,
                                                                  final String description,
                                                                  final RepositoryId subject_id_in,
                                                                  final RepositoryId subject_id_out,
                                                                  final SAMLToken token) {
        HashMap<String, String> form = new HashMap<>();
        form.put("trial_id", trial.id.getId()+"");
        form.put("description", description);
        form.put("subject_id_in", subject_id_in.getId()+"");
        form.put("subject_id_out", subject_id_out.getId() + "");
        form.put("status", "NOT_STARTED");

        return this.apiServer.postForm(API_BASE_URI + "storeExperiment/", token, form)
                .thenApply(JSONObject.class::cast)
                .thenApply(jsonObject -> {
                    final Experiment experiment = new Experiment(token.getUserId(),
                            RepositoryId.fromJsonObj(jsonObject, "id"),
                            jsonObject.getAsString("uuid"),
                            trial);
                    // experiment.setCreatedOn( DatatypeConverter.parseDateTime(jsonObject.getAsString("created_on")).getTime());
                    experiment.description = description;
                    experiment.setCreatedOn(LocalDateTime.now());
                    return experiment;
                });

    }

    /**
     * <p>
     * Creates and stores a new Experiment in the inSilico Trial repository.
     *
     * @param modelId                The id of the hypermodel that this experiment runs
     * @param experiment_description A description of the experiment. It can be the associated "clinical question"..
     * @param patient_pseudonym      The pseudonym of the patient whose data are used in this experiment. This information is
     *                               saved as the "external id" of the input Subject (subject_in)
     * @param token                  The SAMLToken of the principal / user
     * @return The Experiment object.
     */
    public CompletableFuture<Experiment> createNewExperiment(final RepositoryId modelId,
                                                             final String experiment_description,
                                                             final String patient_pseudonym,
                                                             final SAMLToken token) {
        return this.createNewExperiment(modelId, "Trial for " + experiment_description,
                experiment_description, patient_pseudonym, "Inputs", "Outputs", token);
    }

    public CompletableFuture<Experiment> createNewExperiment(final RepositoryId modelId,
                                                             final String trial_description,
                                                             final String experiment_description,
                                                             final String patient_pseudonym,
                                                             final String subject_in_description,
                                                             final String subject_out_description,
                                                             final SAMLToken token) {

        final CompletableFuture<Trial> trial = this.createNewOrReturnExistingTrial(modelId, trial_description, token);
        final CompletableFuture<Subject> subject_in = this.createNewSubject(subject_in_description, patient_pseudonym, token);
        final CompletableFuture<Subject> subject_out = this.createNewSubject(subject_out_description, "", token);
        return CompletableFuture.allOf(trial, subject_in, subject_out)
                .thenCompose(v -> createNewExperimentImpl(trial.join(), experiment_description,
                        subject_in.join().getId(), subject_out.join().getId(), token))
                .thenApply(experiment -> {
                    experiment.setSubjectIn(subject_in.join());
                    experiment.setSubjectOut(subject_out.join());
                    log.info("EXPERIMENT {} created, MODEL = {}, subject: in = {}, out={}",
                            experiment.id, experiment.trial.model_id,
                            subject_in.join().getId(), subject_out.join().getId());
                    return experiment;
                });
    }

    /**
     * <p>
     * Returns an Experiment given its id
     *
     * @param token        The SAMLToken of the principal / user
     * @param experimentId The id (UUID) of the experiment
     * @return The Experiment object.
     */
    public CompletableFuture<Experiment> getExperimentById(final SAMLToken token, final RepositoryId experimentId) {

        return this.apiServer.getJsonAsync(API_BASE_URI + "getExperimentById/", token,
                Collections.singletonMap("id", experimentId.toString()))
                .thenCompose(jsonAware -> {
                    JSONObject jsonExp = (JSONObject) jsonAware;
                    final String uuid = jsonExp.getAsString("uuid");

                    final Experiment exp = new Experiment(token.getUserId(), experimentId, uuid);
                    exp.description = jsonExp.getAsString("description");
                    exp.status = Experiment.EXP_RUN_STATE.fromString(jsonExp.getAsString("status"));
                    final Calendar created_on = DatatypeConverter.parseDateTime(jsonExp.getAsString("created_on"));
                    exp.setCreatedOn(LocalDateTime.ofInstant(created_on.toInstant(), created_on.getTimeZone().toZoneId()));
                    final String modified_on = jsonExp.getAsString("modified_on");
                    if (modified_on != null) {
                        final Calendar parseDateTime = DatatypeConverter.parseDateTime(modified_on);
                        exp.setModifiedOn(LocalDateTime.ofInstant(parseDateTime.toInstant(), parseDateTime.getTimeZone().toZoneId()));
                    }

                    exp.trial = new Trial(RepositoryId.fromJsonObj(jsonExp, "trial_id"));
                    exp.setSubjectIn(new Subject(RepositoryId.fromJsonObj(jsonExp, "subject_id_in")));
                    exp.setSubjectOut(new Subject(RepositoryId.fromJsonObj(jsonExp, "subject_id_out")));
                    return fillExperiment(token, exp);

                    // return jsonToExperiment(jsonExp);
                });
    }

    /**
     * <p>
     * Returns an Experiment given its id
     *
     * @param token        The SAMLToken of the principal / user
     * @param experimentUuid The id (UUID) of the experiment
     * @return The Experiment object.
     */
    public CompletableFuture<Experiment> getExperiment(final SAMLToken token, final UUID experimentUuid) {

        return this.apiServer.getJsonAsync(API_BASE_URI + "getExperimentByUuid/", token,
                Collections.singletonMap("uuid", experimentUuid.toString()))
                .thenCompose(jsonAware -> {
                    JSONObject jsonExp = (JSONObject) jsonAware;
                    final RepositoryId id = RepositoryId.fromJsonObj(jsonExp, "id");

                    final Experiment exp = new Experiment(token.getUserId(), id, experimentUuid.toString());
                    exp.description = jsonExp.getAsString("description");
                    exp.status = Experiment.EXP_RUN_STATE.fromString(jsonExp.getAsString("status"));
                    final Calendar created_on = DatatypeConverter.parseDateTime(jsonExp.getAsString("created_on"));
                    exp.setCreatedOn(LocalDateTime.ofInstant(created_on.toInstant(), created_on.getTimeZone().toZoneId()));
                    final String modified_on = jsonExp.getAsString("modified_on");
                    if (modified_on != null) {
                        final Calendar parseDateTime = DatatypeConverter.parseDateTime(modified_on);
                        exp.setModifiedOn(LocalDateTime.ofInstant(parseDateTime.toInstant(), parseDateTime.getTimeZone().toZoneId()));
                    }

                    exp.trial = new Trial(RepositoryId.fromJsonObj(jsonExp, "trial_id"));
                    exp.setSubjectIn(new Subject(RepositoryId.fromJsonObj(jsonExp, "subject_id_in")));
                    exp.setSubjectOut(new Subject(RepositoryId.fromJsonObj(jsonExp, "subject_id_out")));
                    return fillExperiment(token, exp);

                    // return jsonToExperiment(jsonExp);
                });
    }

    private CompletableFuture<Experiment> fillExperiment(final SAMLToken token, final Experiment exp) {
        final CompletableFuture<Subject> subject_id_in = this.getSubject(token, exp.getSubjectIn().getId());
        final CompletableFuture<Subject> subject_id_out = this.getSubject(token, exp.getSubjectOut().getId());
        final CompletableFuture<Trial> trial = this.getTrialById(token, exp.trial.id);
        return CompletableFuture.allOf(subject_id_in, subject_id_out, trial)
                .thenApply(aVoid -> {
                    exp.setSubjectIn(subject_id_in.join());
                    exp.setSubjectOut(subject_id_out.join());
                    exp.trial = trial.join();
                    return exp;
                });
    }



    /**
     * <p>
     * Returns the experiments created by the given user but "un-filled" i.e. the information
     * about the subjects, files, etc. is missing
     *
     * @return A list of Experiment objects in reverse chronological order, i.e. the most recent is first.
     */
    public CompletableFuture<List<Experiment>> getUserExperiments(final SAMLToken token) {
        return this.apiServer.getJsonAsync(API_BASE_URI + "getUserExperiments/", token)
                .thenApply(JSONObject.class::cast)
                .thenApply(js -> js.values().stream()
                        .map(JSONObject.class::cast)
                        .map(jsonObject -> jsonToExperiment(token, jsonObject))
                        .sorted(Comparator.comparing(Experiment::getStartDateTime).reversed())
                        .collect(Collectors.toList()));
    }

    private Experiment jsonToExperiment(final SAMLToken token, JSONObject jsonExp) {
        final RepositoryId experimentId = RepositoryId.fromJsonObj(jsonExp, "id");
        final String uuid = jsonExp.getAsString("uuid");

        final Experiment exp = new Experiment(token.getUserId(), experimentId, uuid);
        exp.description = jsonExp.getAsString("description");
        exp.status = Experiment.EXP_RUN_STATE.fromString(jsonExp.getAsString("status"));
        //exp.setCreatedOn(DatatypeConverter.parseDateTime(jsonExp.getAsString("created_on")).getTime());
        Calendar parseDateTime = DatatypeConverter.parseDateTime(jsonExp.getAsString("created_on"));
        exp.setCreatedOn(LocalDateTime.ofInstant(parseDateTime.toInstant(), parseDateTime.getTimeZone().toZoneId()));
        final String modified_on = jsonExp.getAsString("modified_on");
        if (modified_on != null) {
            parseDateTime = DatatypeConverter.parseDateTime(modified_on);
            exp.setModifiedOn(LocalDateTime.ofInstant(parseDateTime.toInstant(), parseDateTime.getTimeZone().toZoneId()));
        }

        final JSONObject trial = (JSONObject) jsonExp.get("trial");
        exp.trial = new Trial(RepositoryId.fromJsonObj(trial, "id"), RepositoryId.fromJsonObj(trial,"model_id"));
        final JSONObject subject_in_js = (JSONObject) jsonExp.get("subject_id_in");

        final Subject subjectIn = new Subject(RepositoryId.fromJsonObj(subject_in_js, "id"), subject_in_js.getAsString("description"));
        subjectIn.setExternalId(subject_in_js.getAsString("subject_external_id"));
        exp.setSubjectIn(subjectIn);
        final JSONObject subject_out_js = (JSONObject) jsonExp.get("subject_id_out");
        final Subject subject_out = new Subject(RepositoryId.fromJsonObj(subject_out_js, "id"), subject_out_js.getAsString("description"));
        exp.setSubjectOut(subject_out);
        return exp;
    }


    public CompletableFuture<ByteBuffer> getExperimentInputs(final Experiment experiment, final SAMLToken token)
    {
        return this.getFilesOfSubject(token, experiment.getSubjectIn().getId())
                .thenApply(trFiles -> trFiles.stream().filter(TrFile::isInput).findFirst().map(TrFile::getId))
                .thenCompose(optFileId -> {
                    if (!optFileId.isPresent()) {
                        return FutureUtils.completeExFuture("No inputs in Experiment");
                    }

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    return this.downloadFile(optFileId.get(), baos, token)
                            .thenApply(aVoid -> ByteBuffer.wrap(baos.toByteArray()));
                });
    }

    public CompletableFuture<Boolean> deleteExperiment(final Experiment experiment, final SAMLToken token) {

        return this.apiServer.deleteResourceAsync(API_BASE_URI + "deleteExperimentById/", token,
                Collections.singletonMap("id", experiment.id.toString()))
                .thenCompose(experimentDeleted -> {

                    if (!experimentDeleted)
                        return CompletableFuture.completedFuture(false);

                    final CompletableFuture<Boolean> delSub1Fut =
                            this.apiServer.deleteResourceAsync(API_BASE_URI + "deleteSubjectById/", token,
                                    Collections.singletonMap("id", experiment.getSubjectIn().getId().toString()));
                    final CompletableFuture<Boolean> delSub2Fut =
                            this.apiServer.deleteResourceAsync(API_BASE_URI + "deleteSubjectById/", token,
                                    Collections.singletonMap("id", experiment.getSubjectOut().getId().toString()));

                    return CompletableFuture.allOf(delSub1Fut, delSub2Fut)
                            .thenApply(v -> delSub1Fut.join() && delSub2Fut.join());
                });

    }



}
