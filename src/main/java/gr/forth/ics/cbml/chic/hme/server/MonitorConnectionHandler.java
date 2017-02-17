package gr.forth.ics.cbml.chic.hme.server;

import gr.forth.ics.cbml.chic.hme.server.mq.Messages;
import io.undertow.server.handlers.sse.ServerSentEventConnection;
import io.undertow.server.handlers.sse.ServerSentEventConnectionCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.IoUtils;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

public class MonitorConnectionHandler implements ServerSentEventConnectionCallback {
    private static final Logger log =
            LoggerFactory.getLogger(MonitorConnectionHandler.class);

    private final String userId;
    private final Observable<Messages.ExecutionStatusMessage> observable;

    private ServerSentEventConnection sseConn;
    private Subscription subscription;


    public MonitorConnectionHandler(final String userId,
                                    final Observable<Messages.ExecutionStatusMessage> observable) {
        this.userId = userId;
        this.observable = observable;
    }


    @Override
    public void connected(ServerSentEventConnection sseConn,
                          String s) {
        this.sseConn = sseConn;
        this.sseConn.setKeepAliveTime(10_000);
        sseConn.addCloseTask(serverSentEventConnection -> {
            log.info("closing sse conn for user {}", userId);
            if (this.subscription != null)
                this.subscription.unsubscribe();
        });

        log.info("START MONITORING FOR USER {}", userId);

        this.subscription = observable
                .map(Messages.Message::toJson)
                .observeOn(Schedulers.io())
                .subscribe(jsonObject -> {
                            log.info("SSE: {}", jsonObject);
                            sseConn.send(jsonObject.toJSONString());
                        },
                        ex -> IoUtils.safeClose(sseConn));
    }

}
