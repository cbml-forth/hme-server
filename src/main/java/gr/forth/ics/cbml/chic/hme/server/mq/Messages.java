package gr.forth.ics.cbml.chic.hme.server.mq;

import gr.forth.ics.cbml.chic.hme.server.execution.Experiment;
import lombok.*;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@UtilityClass
@Slf4j
public class Messages {


    private static final Pattern experimentStatusPattern = Pattern.compile("^workflow\\.([a-f0-9-]+)\\.status");

    @Value
    @EqualsAndHashCode(callSuper=false)
    public static class ExecutionStatusMessage extends Message {
        final UUID workflowUUID;
        final Experiment.EXP_RUN_STATE status;

        public Optional<ExecutionStatusMessage> toExecutionStatus() {
            return Optional.of(this);
        }
        public Optional<ModelsChangedMessage> toModelsChange() {
            return Optional.empty();
        }

        @Override
        public JSONObject toJson()
        {

            final String eventType = this.eventType();
            final JSONObject jsonObject = new JSONObject();
            final String id = String.format("%d", this.getId());
            jsonObject.put("id", id);
            jsonObject.put("type", eventType);
            jsonObject.put("uuid", this.getWorkflowUUID().toString());
            jsonObject.put("status", this.getStatus().toString());

            return jsonObject;
        }

        @Override
        public String eventType() {
            return "experiment.change";
        }

    }
    @Value
    @EqualsAndHashCode(callSuper=false)
    public static class ModelsChangedMessage extends Message {
        enum Type {NEW, CHANGED, DELETED}
        enum Change {BASIC, PARAMETERS, FILES, REFERENCES, PERSPECTIVES, NO_CHANGE}

        final UUID modelUUID;
        final Type type;
        final Change change;

        public Optional<ExecutionStatusMessage> toExecutionStatus() {
            return Optional.empty();
        }
        public Optional<ModelsChangedMessage> toModelsChange() {
            return Optional.of(this);
        }
        @Override
        public JSONObject toJson()
        {

            final String eventType = this.eventType();
            final JSONObject jsonObject = new JSONObject();
            final String id = String.format("%d", this.getId());
            jsonObject.put("id", id);
            jsonObject.put("type", eventType);
            jsonObject.put("uuid", this.modelUUID.toString());

            return jsonObject;
        }
        @Override
        public String eventType() {
            return "models." + type + "." + change;
        }
    }

    @EqualsAndHashCode
    public static abstract class Message {
        final Instant tm;
        @Getter @Setter(AccessLevel.PACKAGE)
        Long id;

        Message()
        {
            this.tm = Instant.now();
            this.id = 0L;
        }

        public long getTimeStamp(){
            return this.tm.toEpochMilli();
        }

        abstract public Optional<ModelsChangedMessage> toModelsChange();
        abstract public Optional<ExecutionStatusMessage> toExecutionStatus();
        abstract public JSONObject toJson();
        abstract public String eventType();
    }


    static private Messages.ExecutionStatusMessage toExecutionStatus(final String subject, final String message)
    {
        Matcher matcher = experimentStatusPattern.matcher(subject);
        if (matcher.matches()) {
            final UUID workflowUuid = UUID.fromString(matcher.group(1));
            final Experiment.EXP_RUN_STATE status = Experiment.EXP_RUN_STATE.fromString(message);
            log.info("--> [VPHHF: {} : {}]", workflowUuid, message);
            return new Messages.ExecutionStatusMessage(workflowUuid, status);
        }
        return null;
    }
    static private Messages.ModelsChangedMessage toModelsChange(final String subject, final String message) {

        /*

| Repository Change                                   | Routing Key                 | Body                                             |
| --------------------------------------------------- | --------------------------- | ----------------------------------------         |
| Addition of a new model/tool                        | models.new                  | uuid of the new model/tool                       |
| Addition/change/deletion of model/tool parameter    | models.changed.parameters   | uuid of the model to which the parameter belongs |
| Addition/change/deletion of model/tool reference    | models.changed.references   | uuid of the model to which the reference is linked |
| Addition/deletion of model/tool file                | models.changed.files        | uuid of the model to which the file belongs      |
| Addition/change/deletion of model perspective value | models.changed.perspectives | uuid of the model with which the given perspective value is associated |
| Deletion of model/tool                              | models.deleted              | uuid of the model that has been deleted          |

         */

            ModelsChangedMessage.Type type;
            ModelsChangedMessage.Change what;
            switch (subject) {
                case "models.new":
                    type = ModelsChangedMessage.Type.NEW;
                    what = ModelsChangedMessage.Change.NO_CHANGE;
                    break;
                case "models.deleted":
                    type = ModelsChangedMessage.Type.DELETED;
                    what = ModelsChangedMessage.Change.NO_CHANGE;
                    break;
                case "models.changed":
                    type = ModelsChangedMessage.Type.CHANGED;
                    what = ModelsChangedMessage.Change.BASIC;
                    break;
                case "models.changed.parameters":
                    type = ModelsChangedMessage.Type.CHANGED;
                    what = ModelsChangedMessage.Change.PARAMETERS;
                    break;
                case "models.changed.files":
                    type = ModelsChangedMessage.Type.CHANGED;
                    what = ModelsChangedMessage.Change.FILES;
                    break;
                case "models.changed.perspectives":
                    type = ModelsChangedMessage.Type.CHANGED;
                    what = ModelsChangedMessage.Change.PERSPECTIVES;
                    break;
                case "models.changed.references":
                    type = ModelsChangedMessage.Type.CHANGED;
                    what = ModelsChangedMessage.Change.REFERENCES;
                    break;
                default:
                    return null;
            }
            // The body of the message always contains the UUID of the affected model
            final UUID modelUUID = UUID.fromString(message);
            return new Messages.ModelsChangedMessage(modelUUID, type, what);

    }


    static public Optional<Message> of (final String routingKey, final String message)
    {

        Messages.Message event = null;
        final Messages.ExecutionStatusMessage executionStatusMessage = toExecutionStatus(routingKey, message);
        if (executionStatusMessage != null)
            event = executionStatusMessage;
        else {
            final Messages.ModelsChangedMessage modelsChangedMessage = toModelsChange(routingKey, message);
            if (modelsChangedMessage != null)
                event = modelsChangedMessage;
        }
        return Optional.ofNullable(event);
    }
}
