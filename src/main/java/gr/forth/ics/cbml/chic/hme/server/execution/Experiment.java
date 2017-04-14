package gr.forth.ics.cbml.chic.hme.server.execution;

import gr.forth.ics.cbml.chic.hme.server.modelrepo.RepositoryId;
import lombok.Data;
import net.minidev.json.JSONObject;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by ssfak on 31/1/17.
 */
@Data
public class Experiment {
    public Map<String, String> input_values;

    public enum EXP_RUN_STATE {
        NOT_STARTED, RUNNING, FINISHED_FAIL, FINISHED_OK;

        public static EXP_RUN_STATE fromString(String text) {
            if ("NOT STARTED".equalsIgnoreCase(text) || "NOT_STARTED".equalsIgnoreCase(text))
                return NOT_STARTED;
            if ("ON PROGRESS".equalsIgnoreCase(text) || "ON_PROGRESS".equalsIgnoreCase(text) || "IN_PROGRESS".equalsIgnoreCase(text) || "RUNNING".equalsIgnoreCase(text))
                return RUNNING;
            if ("FINISHED_OK".equalsIgnoreCase(text) || "FINISHED SUCCESSFULLY".equalsIgnoreCase(text))
                return FINISHED_OK;
            if ("FAILED".equalsIgnoreCase(text) || "FINISHED ERRONEOUSLY".equalsIgnoreCase(text))
                return FINISHED_FAIL;
            return NOT_STARTED;
        }
    }

    final RepositoryId id;
    final String uuid;
    final String user_uuid;

    UUID workflow_uuid;
    Trial trial;
    String description;
    String clinicalQuestion = "";
    Subject subjectIn;
    Subject subjectOut;
    EXP_RUN_STATE status;
    Path outputs_path;
    LocalDateTime createdOn; // The Date it was stored in the Repository as reported by the repo itself.
    LocalDateTime modifiedOn; // The Date it was stored in the Repository as reported by the repo itself.

    public Experiment(final String user_uuid, final RepositoryId id, final String uuid) {
        this(user_uuid, id, uuid, null);
    }

    public Experiment(final String user_uuid, final RepositoryId id, final String uuid, final Trial trial) {
        this.id = id;
        this.uuid = uuid;
        this.user_uuid = user_uuid;
        this.trial = trial;
        this.status = EXP_RUN_STATE.NOT_STARTED;
        this.input_values = Collections.emptyMap();
    }

    public List<TrFile> getOutputFiles() {
        return this.subjectOut.getFiles();
    }

    public String getPatientPseudonym() {
        return this.subjectIn.getExternalId();
    }
/*
    public CompletableFuture<String> addInputs(final ByteBuffer b, final SAMLToken token) {
        Objects.requireNonNull(this.subject_in);
        return this.subject_in.uploadFile("inputs", "baclava", b, token);
    }
    public CompletableFuture<String> addInputs(final ByteBuffer b) {
        Objects.requireNonNull(this.subject_in);
        return this.subject_in.uploadFile("inputs", "baclava", b);
    }

    public CompletableFuture<ByteBuffer> getInputs() {
        Objects.requireNonNull(this.subject_in);
        final Optional<String> inFileOpt = this.subject_in.files.stream()
                .filter(TrFile::isInput)
                .map(TrFile::getId)
                .findFirst();
        if (!inFileOpt.isPresent())
            return FutureUtils.completeExFuture(new FileNotFoundException("No Baclava file found"));
        final String inputFileId = inFileOpt.get();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        return this.subject_in.downloadFile(inputFileId, baos)
                .thenApply(aVoid -> ByteBuffer.wrap(baos.toByteArray()));
    }

    public CompletableFuture<String> addReport(final ByteBuffer b) {
        Objects.requireNonNull(this.subject_out);

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
        df.setTimeZone(tz);
        String nowFormatted = df.format(new Date()); // XXX: Hack, to create unique names
        return this.subject_out.uploadFile("PDF Report ("+nowFormatted+")", "report", b);
    }*/



    /*
        public List<String> reportFileIds()
        {
            Objects.requireNonNull(this.subject_out);
            return this.subject_out.files.stream()
                    .filter(TrFile::isReport)
                    .map(TrFile::getId)
                    .collect(Collectors.toList());
        }

        public CompletableFuture<Path> downloadReport() {
            return this.subject_out.updateFiles()
                    .thenCompose(aVoid -> this.downloadReport_i());
        }

        private CompletableFuture<Path> downloadReport_i() {
            Objects.requireNonNull(this.subject_out);
            // Find the file that is the report:
            // We sort the files based on creation date in rev. order
            // and therefore the first will be the file most recently created.
            final Optional<TrFile> fileOptional = this.subject_out.files.stream()
                    .filter(TrFile::isReport)
                    .findFirst();

            if (!fileOptional.isPresent())
                return CompletableFuture.completedFuture(null);

            final String fileId = fileOptional.get().getId();
            try {
                final Path report = Files.createTempFile("report", ".pdf");
                final java.io.File file = report.toFile();
                final FileOutputStream fileOutputStream = new FileOutputStream(file);
                return this.subject_out.downloadFile(fileId, fileOutputStream)
                        .thenApply(aVoid -> report);
            } catch (IOException e) {
                e.printStackTrace();
                return FutureUtils.completeExFuture(e);
            }

        }
    */


    public boolean hasFinished() {
        return this.status == EXP_RUN_STATE.FINISHED_FAIL || this.status == EXP_RUN_STATE.FINISHED_OK;
    }

    public boolean hasSuccessfullyFinished() {
        return this.status == EXP_RUN_STATE.FINISHED_OK;
    }


    public LocalDateTime getStartDateTime() {
        return createdOn;
    }

    public LocalDateTime getFinishDateTime() {
        if (this.hasFinished())
            return modifiedOn;
        return null;
    }


    public static String formatDateISO(final LocalDateTime d) {
        return DateTimeFormatter.ISO_INSTANT.format(d.toInstant(ZoneOffset.UTC));
    }

    public JSONObject toJson() {
        final Experiment experiment = this;
        JSONObject js = new JSONObject();
        final long id = experiment.id.id;
        js.put("id", id);
        js.put("uuid", experiment.uuid);
        js.put("workflow_uuid", experiment.workflow_uuid.toString());
        js.put("user_uid", experiment.user_uuid);
        js.put("patient_pseudoid", experiment.getPatientPseudonym());
        js.put("description", experiment.description);
        js.put("clinical_question", experiment.getClinicalQuestion());
        js.put("model_id", experiment.trial.model_id);
        js.put("subject_in", experiment.getSubjectIn().getId());
        js.put("subject_out", experiment.getSubjectOut().getId());
        js.put("start_datetime", formatDateISO(experiment.getStartDateTime()));
        if (experiment.hasFinished())
            js.put("finish_datetime", formatDateISO(experiment.getFinishDateTime()));
        js.put("status", experiment.status.toString());
        js.put("trial", experiment.trial.toJson());
        return js;
    }
}