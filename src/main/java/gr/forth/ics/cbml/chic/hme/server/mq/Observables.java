package gr.forth.ics.cbml.chic.hme.server.mq;

import com.github.pgasync.Db;
import com.github.pgasync.Row;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.observables.ConnectableObservable;
import rx.subjects.PublishSubject;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class Observables implements AutoCloseable{
    private final Db db;

    private final PublishSubject<Messages.Message> publisher;
    private final Observable<Messages.ModelsChangedMessage> modelMessages;
    private final Observable<Messages.ExecutionStatusMessage> executionMessages;

    Observables(Db db) {
        this.db = db;
        this.publisher = PublishSubject.create();
        final ConnectableObservable<Messages.Message> observable =
                this.publisher.publish();
        this.modelMessages = observable
                .filter(msg -> msg.toModelsChange().isPresent())
                .map(msg -> msg.toModelsChange().get())
                .doOnNext(msg-> log.info("+++ "+msg))
                .share();
        this.executionMessages = observable
                .filter(msg -> msg.toExecutionStatus().isPresent())
                .map(msg -> msg.toExecutionStatus().get())
                //.flatMap(this::updateExperiment)
                .doOnNext(msg-> log.info("--- "+msg))
                // share is important here so the DB queries to get the corresponding
                // experiment are not performed multiple times, once for each Subscriber
                // Cf. http://blog.danlew.net/2016/06/13/multicasting-in-rxjava/
                .share();
        observable.connect();
    }

    CompletableFuture<Messages.Message> handle(Messages.Message msg)
    {

        final String aggregate_type = msg.toExecutionStatus().isPresent() ? "experiment" : "model";
        final UUID uuid = msg.toExecutionStatus().isPresent() ? msg.toExecutionStatus().get().getWorkflowUUID()
                : msg.toModelsChange().get().getModelUUID();
        final String event_type = msg.eventType();
        final String jsonData = msg.toJson().toJSONString();
        CompletableFuture<Messages.Message> fut = new CompletableFuture<>();
        db.query("INSERT INTO events(event_type,aggregate_type,aggregate_uuid,data)" +
                " VALUES($1,$2,$3,$4) RETURNING event_id",
                Arrays.asList(event_type, aggregate_type, uuid, jsonData),
                resultSet -> {
                    final Row row = resultSet.row(0);
                    final Long eventId = row.getLong("event_id");
                    msg.setId(eventId);
                    fut.complete(msg);
                    this.publisher.onNext(msg);
                },
                fut::completeExceptionally);
        return fut;
    }

    /**
     * Returns an observable for the new messages
     * @return Observable of Messages
     */
    public Observable<Messages.Message> messages() {
        return this.publisher;
    }


    public Observable<Messages.ModelsChangedMessage> modelMessages() {
        return this.modelMessages;
    }
    public Observable<Messages.ExecutionStatusMessage> executionMessages() {
        return this.executionMessages;
    }
/*

    private Observable<Experiment> updateExperiment(Messages.ExecutionStatusMessage msg)
    {
        ExperimentRepository a = new ExperimentRepository(3);
        return FutureUtils.futureToObservable(a.getExperiment(token, msg.getWorkflowUUID()));

    }*/

    @Override
    public void close() throws Exception {
        this.publisher.onCompleted();
    }
}
