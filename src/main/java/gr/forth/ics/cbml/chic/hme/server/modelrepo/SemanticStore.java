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
package gr.forth.ics.cbml.chic.hme.server.modelrepo;

import com.ning.http.client.*;
import com.opencsv.CSVReader;

import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

/**
 * Created by ssfak on 5/12/16.
 */
public class SemanticStore implements  AutoCloseable {

    private final String serverSparqlEndpoint;


    private final AsyncHttpClient asyncHttpClient;

    public SemanticStore(String endpoint) {
        this.serverSparqlEndpoint = endpoint;
        this.asyncHttpClient = new AsyncHttpClient(new AsyncHttpClientConfig.Builder()
                .setAllowPoolingConnections(true)
                // .setMaxConnectionsPerHost(100)
                .build());
    }


    CompletableFuture<String> send_query(final String query) {
        final CompletableFuture<String> fut = new CompletableFuture<>();
        Map<String, List<String>> params = new HashMap<>();
        params.put("query", Arrays.asList(query));
        // params.put("default-graph-uri", Arrays.asList(default_graph_uri));

        Request r = new RequestBuilder().setUrl(this.serverSparqlEndpoint)
                .setMethod("POST")
                .setFormParams(params)
                .setHeader("Accept", "text/csv")
                .build();
        asyncHttpClient.prepareRequest(r).execute(new AsyncCompletionHandler<Void>() {
            @Override
            public Void onCompleted(Response response) throws Exception {
                // System.out.println("Response returned " + response.getStatusCode() + " at thread " + Thread.currentThread().getId() + "\n");
                if (response.getStatusCode() / 100 == 2) // i.e. 2xx
                    fut.complete(response.getResponseBody());
                else
                    fut.completeExceptionally(new Throwable(response.getResponseBody()));
                return null;
            }

            @Override
            public void onThrowable(Throwable t) {
                System.out.println("Response error " + t.getMessage());
                fut.completeExceptionally(t);
            }
        });
        return fut;
    }

    public CompletableFuture<List<Map<String, String>>> send_query_and_parse(final String query) {
        return this.send_query(query)
                .thenApply(csv -> {
                    // System.out.printf("Got:\n"+csv);
                    CSVReader reader = new CSVReader(new StringReader(csv));
                    try {
                        String[] colNames = reader.readNext();
                        List<Map<String, String>> records =
                                reader.readAll().stream().map(cols -> {
                                    Map<String, String> obj = new HashMap<>();
                                    for (int i = 0; i < cols.length; i++) {
                                        obj.put(colNames[i], cols[i]);
                                    }
                                    return obj;
                                }).collect(toList());
                        return records;
                    } catch (Throwable ex) {
                        throw new RuntimeException(ex.getMessage());
                    }
                });
    }


    @Override
    public void close() throws Exception {
        this.asyncHttpClient.close();
    }
}
