package gr.forth.ics.cbml.chic.hme.server.mq;

import com.github.pgasync.Db;
import com.github.pgasync.Row;
import gr.forth.ics.cbml.chic.hme.server.execution.Experiment;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import rx.Observable;
import rx.observables.ConnectableObservable;
import rx.subjects.PublishSubject;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class Observables implements AutoCloseable {

    private final PublishSubject<Messages.Message> publisher;
    private final Observable<Messages.ModelsChangedMessage> modelMessages;
    private final Observable<JSONObject> executionMessages;
    private final Db db;

    Observables(final Db db) {
        this.db = db;
        this.publisher = PublishSubject.create();
        final ConnectableObservable<Messages.Message> observable =
                this.publisher.publish();
        this.modelMessages = observable
                .filter(msg -> msg.toModelsChange().isPresent())
                .map(msg -> msg.toModelsChange().get())
                .doOnNext(msg -> log.info("+++ " + msg))
                .share();
        this.executionMessages = observable
                .filter(msg -> msg.toExecutionStatus().isPresent())
                .map(msg -> msg.toExecutionStatus().get())
                .flatMap(this::updateAndReturnExperiment)
                // share is important here so the DB queries to get the corresponding
                // experiment are not performed multiple times, once for each Subscriber
                // Cf. http://blog.danlew.net/2016/06/13/multicasting-in-rxjava/
                .share();
        observable.connect();
        this.executionMessages.subscribe(msg -> log.info("--- " + msg));
    }

    private Observable<JSONObject> updateAndReturnExperiment(Messages.ExecutionStatusMessage message) {
        final UUID workflowUUID = message.getWorkflowUUID();
        final Experiment.EXP_RUN_STATE status = message.getStatus();
        log.info("Updating Experiment {} to be {}", workflowUUID, status);
        return db.queryRows(
                "UPDATE experiments SET status=$1 WHERE workflow_uuid=$2" +
                        " RETURNING data::text", status.toString(), workflowUUID)
                .doOnError(throwable -> log.info("updating experiment ", throwable))
                .map(row -> {
                    JSONParser p = new JSONParser(JSONParser.MODE_RFC4627);
                    JSONObject o = null;
                    try {
                        o = (JSONObject) p.parse(row.getString(0));
                        o.put("status", status.toString());
                        o.put("event_id", message.getId());
                    } catch (ParseException e) {}
                    return o;
                })
                .filter(Objects::nonNull);
    }

    void publish(Messages.Message msg) {
        this.publisher.onNext(msg);
    }

    /**
     * Returns an observable for the new messages
     *
     * @return Observable of Messages
     */
    public Observable<Messages.Message> messages() {
        return this.publisher;
    }


    public Observable<Messages.ModelsChangedMessage> modelMessages() {
        return this.modelMessages;
    }

    public Observable<JSONObject> executionMessages() {
        return this.executionMessages;
    }

    @Override
    public void close() throws Exception {
        this.publisher.onCompleted();
    }
}
