package gr.forth.ics.cbml.chic.hme.server;

import com.github.pgasync.Db;
import io.undertow.server.handlers.sse.ServerSentEventConnection;
import io.undertow.server.handlers.sse.ServerSentEventConnectionCallback;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.IoUtils;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.math.BigInteger;
import java.util.Objects;

public class MonitorConnectionHandler implements ServerSentEventConnectionCallback {
    private static final Logger log =
            LoggerFactory.getLogger(MonitorConnectionHandler.class);

    private final String userId;
    private final Observable<JSONObject> observable;
    private long lastEventId;
    private final Db db;

    private ServerSentEventConnection sseConn;
    private Subscription subscription;


    public MonitorConnectionHandler(final String userId,
                                    final Observable<JSONObject> observable,
                                    long lastEventId,
                                    final Db db) {
        this.userId = userId;
        this.observable = observable;
        this.lastEventId = lastEventId;
        this.db = db;
    }


    private Observable<JSONObject> lastEvents() {
        if (lastEventId < 0)
            return Observable.empty();

        return db.queryRows(
                "SELECT event_id::TEXT, experiments.data::text " +
                        " FROM events JOIN experiments ON (experiment_uid=aggregate_uuid)" +
                        " WHERE event_id>$1 ORDER BY event_id ASC", lastEventId)
                .map(row -> {
                    JSONParser p = new JSONParser(JSONParser.MODE_RFC4627);
                    JSONObject o = null;
                    try {
                        final String event_id = row.getString(0);
                        o = (JSONObject) p.parse(row.getString(1));
                        o.put("event_id", Long.parseLong(event_id));
                    } catch (ParseException e) {}
                    return o;
                })
                .doOnError(throwable -> log.info("retrieving missed experiments", throwable))
                .filter(Objects::nonNull);
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
                //.map(Messages.Message::toJson)
                .startWith(lastEvents())
                .observeOn(Schedulers.io())
                .filter(jsonObject -> userId.equals(jsonObject.getAsString("user_uid")))
                .subscribe(jsonObject -> {
                            log.info("SSE: {}", jsonObject);
                            sseConn.send(jsonObject.toJSONString(), "execution", jsonObject.getAsString("event_id"), null);
                        },
                        ex -> IoUtils.safeClose(sseConn));
    }

}
