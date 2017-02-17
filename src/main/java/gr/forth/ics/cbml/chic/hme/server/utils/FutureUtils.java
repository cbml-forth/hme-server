/*
 * Copyright 2016-2017 FORTH-ICS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gr.forth.ics.cbml.chic.hme.server.utils;

import lombok.experimental.UtilityClass;
import rx.Observable;
import rx.Single;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

/**
 * Created by ssfak on 29/12/15.
 */
@UtilityClass
public class FutureUtils {

    public static <T> CompletableFuture<T> fromObservable(Observable<T> observable) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        observable
                .doOnError(future::completeExceptionally)
                .first()
                .forEach(future::complete);
        return future;
    }

    public static <T> Observable<T> futureToObservable(CompletableFuture<T> fut) {
        return Observable.create(subscriber ->
                fut.whenComplete((result, error) -> {
                    if (error != null) {
                        subscriber.onError(error);
                    } else {
                        subscriber.onNext(result);
                        subscriber.onCompleted();
                    }
                }));

    }
    public static <T> Single<T> futureToSingle(CompletableFuture<T> future) {
        return Single.create(subscriber ->
                future.whenComplete((result, error) -> {
                    if (error != null) {
                        subscriber.onError(error);
                    } else {
                        subscriber.onSuccess(result);
                    }
                }));
    }

    public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
        CompletableFuture<Void> allDoneFuture =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        return allDoneFuture.thenApply(v ->
                futures.stream().
                        map(CompletableFuture::join).
                        collect(toList()));
    }

    private static <T, R> void waterfall_i(List<T> data,
                                           CompletableFuture<List<R>> future,
                                           Function<T, CompletableFuture<R>> process,
                                           List<R> results_so_far,
                                           int concurrency) {
        if (data.size() == 0) {
            future.complete(results_so_far);
            return;
        }
        final int len = concurrency < data.size() ? concurrency : data.size();
        final List<T> subList = data.subList(0, len);
        final List<CompletableFuture<R>> futures = subList.stream()
                .map(process)
                .collect(toList());

        final CompletableFuture<List<R>> future_i = sequence(futures);
        future_i.handle((List<R> ts, Throwable ex) -> {
            if (ex != null)
                future.completeExceptionally(ex);
            else {
                results_so_far.addAll(ts);
                waterfall_i(data.subList(len, data.size()), future, process, results_so_far, concurrency);
            }
            return null;
        });

    }

    public static <T, R> CompletableFuture<List<R>> waterfall(List<T> data,
                                                              Function<T, CompletableFuture<R>> process,
                                                              int concurrency) {

        CompletableFuture<List<R>> future = new CompletableFuture<>();
        List<R> results_so_far = new ArrayList<>(data.size());
        waterfall_i(data, future, process, results_so_far, concurrency);
        return future;

    }

    public static <U, V, T> CompletableFuture<T> thenComposeBoth(CompletableFuture<? extends U> one,
                                                                 CompletableFuture<? extends V> another,
                                                                 BiFunction<? super U, ? super V, ? extends CompletableFuture<T>> fn) {
        return CompletableFuture.allOf(one, another)
                .thenCompose(__ -> fn.apply(one.join(), another.join()));

    }

    public static <T> CompletableFuture<T> completeExFuture(final String message) {
        return completeExFuture(new Throwable(message));
    }
    public static <T> CompletableFuture<T> completeExFuture(Throwable ex) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
    }
}
