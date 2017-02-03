package gr.forth.ics.cbml.chic.hme.server.utils;

import com.github.pgasync.Db;
import com.github.pgasync.ResultSet;
import com.github.pgasync.Row;
import lombok.experimental.UtilityClass;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@UtilityClass
public class DbUtils {

    public static CompletableFuture<ResultSet> queryDb(Db db,
                                                       String sql,
                                                       List params) {
        CompletableFuture<ResultSet> fut = new CompletableFuture<>();
        db.query(sql, params, fut::complete, fut::completeExceptionally);
        return fut;
    }

    public static CompletableFuture<ResultSet> queryDb(Db db,
                                                       String sql) {
        return queryDb(db, sql, Collections.emptyList());

    }

    public static CompletableFuture<Optional<Row>> queryOneDb(Db db,
                                                              String sql,
                                                              List params) {
        return queryDb(db, sql, params)
                .thenApply(rs -> {
                    if (rs.size() == 0) {
                        return Optional.empty();
                    }
                    return Optional.of(rs.row(0));
                });
    }

    public static CompletableFuture<Optional<Row>> queryOneDb(Db db,
                                                              String sql) {
        return queryOneDb(db, sql, Collections.emptyList());
    }
}
