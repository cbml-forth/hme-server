package gr.forth.ics.cbml.chic.hme.server.mq;

import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.observables.ConnectableObservable;
import rx.subjects.PublishSubject;

@Slf4j
public class Observables implements AutoCloseable{

    private final PublishSubject<Messages.Message> publisher;
    private final Observable<Messages.ModelsChangedMessage> modelMessages;
    private final Observable<Messages.ExecutionStatusMessage> executionMessages;

    Observables() {
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

    public void publish(Messages.Message msg)
    {
        this.publisher.onNext(msg);
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

    @Override
    public void close() throws Exception {
        this.publisher.onCompleted();
    }
}
