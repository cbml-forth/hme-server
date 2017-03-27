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
package gr.forth.ics.cbml.chic.hme.server;

import com.ning.http.client.*;
import com.ning.http.client.multipart.ByteArrayPart;
import com.ning.http.client.multipart.FilePart;
import com.ning.http.client.multipart.StringPart;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONAware;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import nu.xom.Builder;
import nu.xom.Element;
import rx.Emitter;
import rx.Observable;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public class WebApiServer implements AutoCloseable {
    final AsyncHttpClient httpClient_;
    private ExecutorService exec = Executors.newSingleThreadExecutor();

    private final int max_req_in_flight;
    private final Semaphore req_in_flight;


    // Statistics
    private final AtomicInteger total_req = new AtomicInteger(0);
    private final AtomicInteger failed_req = new AtomicInteger(0);

    public WebApiServer(final int max_req_in_flight) {
        this.max_req_in_flight = max_req_in_flight;
        this.req_in_flight = new Semaphore(this.max_req_in_flight, true);
        this.httpClient_ = new AsyncHttpClient(new AsyncHttpClientConfig.Builder()
                .setAllowPoolingConnections(true)
                .setMaxConnectionsPerHost(20)
                .setAcceptAnyCertificate(true)
                .build());
    }

    public int concurrency()
    {
        return this.max_req_in_flight;
    }
    public int currentlyPendingReqs()
    {
        return this.max_req_in_flight - this.req_in_flight.availablePermits();
    }
    public CompletableFuture<JSONAware> getJsonAsync(final String url,
                                                     final SAMLToken token) {
        return this.getJsonAsync(url, token, Collections.emptyMap());
    }

    public CompletableFuture<JSONAware> getJsonAsync(final String url,
                                                     final SAMLToken token,
                                                     final Map<String, String> qs) {
        final AsyncHttpClient.BoundRequestBuilder builder = this.httpClient_.prepareGet(url)
                .addHeader("Authorization", token.getHttpAuthzHeader());
        for (String n : qs.keySet()) {
            builder.addQueryParam(n, qs.get(n));
        }
        final Request request = builder.build();
        if (request.getContentLength() <= 0)
            request.getHeaders().put("Content-Length", Arrays.asList("0"));

        final CompletableFuture<JSONAware> fut = new CompletableFuture<>();
        final Exception clientTrace = clientTrace();
        exec.submit(() -> executeJSONRequest(request, fut, clientTrace));
        return fut;
    }


    private Exception clientTrace() {
        return new Exception("Client stack trace");
    }

    public CompletableFuture<Boolean> deleteResourceAsync(final String url,
                                                          final SAMLToken token,
                                                          final Map<String, String> qs) {
        final AsyncHttpClient.BoundRequestBuilder builder = this.httpClient_.prepareDelete(url)
                .addHeader("Authorization", token.getHttpAuthzHeader());
        for (String n : qs.keySet()) {
            builder.addQueryParam(n, qs.get(n));
        }
        final Request request = builder.build();
        final Exception clientTrace = clientTrace();
        final CompletableFuture<Boolean> fut = new CompletableFuture<>();
        exec.submit(() -> {
            try {
                req_in_flight.acquire();
                httpClient_.executeRequest(request,
                        new AsyncCompletionHandler<Response>() {
                            @Override
                            public void onThrowable(Throwable t) {
                                req_in_flight.release();
                                t.setStackTrace(clientTrace.getStackTrace());

                                log.error("ERROR EX: {} for {}", t, request.getUrl());
                                fut.completeExceptionally(t);
                            }

                            @Override
                            public Response onCompleted(Response response) throws Exception {
                                req_in_flight.release();
                                fut.complete(response.getStatusCode() / 100 == 2);
                                return response;
                            }

                        });
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        return fut;
    }

    public CompletableFuture<Element> postXMLAsync(final String url,
                                                   final SAMLToken token,
                                                   final String body) {
        return this.postXMLAsync(url, token, body, Collections.emptyMap());
    }

    public CompletableFuture<Element> postXMLAsync(final String url,
                                                   final SAMLToken token,
                                                   final String body,
                                                   final Map<String, String> qs) {
        final AsyncHttpClient.BoundRequestBuilder builder = this.httpClient_.preparePost(url)
                .addHeader("Content-type", "application/xml")
                .addHeader("Authorization", token.getHttpAuthzHeader());
        for (String n : qs.keySet()) {
            builder.addQueryParam(n, qs.get(n));
        }
        builder.setBody(body);
        final Request request = builder.build();

        final CompletableFuture<InputStream> fut = new CompletableFuture<>();
        final Exception clientTrace = clientTrace();
        final CompletableFuture<Element> future = fut.thenApply(responseStream -> {
            try {
                Element root = new Builder().build(responseStream).getRootElement();
                return root;

            } catch (Exception e) {
                final RuntimeException exception = new RuntimeException("XML parsing failed", e);
                exception.setStackTrace(clientTrace.getStackTrace());
                throw exception;
            }
        });
        exec.submit(() -> executeRequestAndParseResponse(request, fut, clientTrace));
        return future;
    }


    public CompletableFuture<Void> downloadContent(final String url,
                                                   final SAMLToken token,
                                                   final Map<String, String> qs,
                                                   final OutputStream whereToSave) {
        final AsyncHttpClient.BoundRequestBuilder builder = this.httpClient_.prepareGet(url)
                .addHeader("Authorization", token.getHttpAuthzHeader());
        for (String n : qs.keySet()) {
            builder.addQueryParam(n, qs.get(n));
        }
        final Request request = builder.build();
        final CompletableFuture<Void> fut = new CompletableFuture<>();
        final WebApiServer self = this;
        final Exception clientTrace = clientTrace();
        exec.submit(() -> {
            try {
                req_in_flight.acquire();
                httpClient_.executeRequest(request,
                        new AsyncCompletionHandler<Response>() {
                            @Override
                            public void onThrowable(Throwable t) {
                                req_in_flight.release();
                                t.setStackTrace(clientTrace.getStackTrace());
                                log.error("ERROR EX: {} for {}", t, request.getUrl());
                                fut.completeExceptionally(t);
                                self.total_req.incrementAndGet();
                                self.failed_req.incrementAndGet();
                            }


                            @Override
                            public STATE onStatusReceived(HttpResponseStatus status) throws Exception {
                                if (status.getStatusCode() / 100 != 2) {
                                    log.error("ERROR: {} for {}", status.getStatusText(), request.getUrl());
                                    final RuntimeException ex = new RuntimeException("API returned: " + status.getStatusText());
                                    ex.setStackTrace(clientTrace.getStackTrace());
                                    fut.completeExceptionally(ex);
                                    return STATE.ABORT;
                                }
                                return super.onStatusReceived(status);
                            }

                            @Override
                            public STATE onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
                                whereToSave.write(content.getBodyPartBytes());
                                return STATE.CONTINUE;
                            }

                            @Override
                            public Response onCompleted(Response response) throws Exception {
                                req_in_flight.release();
                                self.total_req.incrementAndGet();
                                whereToSave.flush();
                                whereToSave.close();
                                if (response.getStatusCode() == 200) {
                                    fut.complete(null);
                                }
                                return response;
                            }
                        });
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        return fut;
    }


    public CompletableFuture<JSONAware> postForm(final String url,
                                                 final SAMLToken token,
                                                 final Map<String, String> formEntries) {
        return this.postForm(url, token, formEntries, Collections.emptyMap());
    }


    public CompletableFuture<JSONAware> postFormMultipart(final String url,
                                                          final SAMLToken token,
                                                          final Map<String, String> formEntries) {
        return this.postFormMultipart(url, token, formEntries, true);
    }

    public CompletableFuture<JSONAware> postFormMultipart(final String url,
                                                          final SAMLToken token,
                                                          final Map<String, String> formEntries,
                                                          final boolean postMethod) {
        final AsyncHttpClient.BoundRequestBuilder builder =
                postMethod ? this.httpClient_.preparePost(url) : this.httpClient_.preparePut(url);

        builder.addHeader("Authorization", token.getHttpAuthzHeader());

        for (String n : formEntries.keySet()) {
            builder.addBodyPart(new StringPart(n, formEntries.get(n), "text/plain", StandardCharsets.UTF_8));
        }


        final Request request = builder.build();
        if (request.getContentLength() == 0)
            request.getHeaders().put("Content-Length", Arrays.asList("0"));
        final CompletableFuture<JSONAware> fut = new CompletableFuture<>();
        final Exception clientTrace = clientTrace();
        exec.submit(() -> executeJSONRequest(request, fut, clientTrace));
        return fut;
    }

    public Observable<JSONAware> uploadFile(final String url,
                                            final SAMLToken token,
                                            final File file,
                                            final String paramName) {
        return this.uploadFile(url, token, file, paramName, file.getName());
    }

    public Observable<JSONAware> uploadFile(final String url,
                                            final SAMLToken token,
                                            final File file,
                                            final String paramName,
                                            final String fileName) {
        final AsyncHttpClient.BoundRequestBuilder builder = this.httpClient_.preparePost(url)
                .addHeader("Authorization", token.getHttpAuthzHeader());
        builder.addBodyPart(new FilePart(paramName, file, null, null, fileName));

        final Request request = builder.build();
        return Observable.fromEmitter(emitter -> emitJSONResponse(request, emitter), Emitter.BackpressureMode.BUFFER);


    }

    public Observable<JSONAware> uploadFileChunk(final String url,
                                                 final SAMLToken token,
                                                 final byte[] buffer,
                                                 final String paramName,
                                                 final String fileName) {

        // System.err.println("[uploadFileChunk at " + Thread.currentThread().getName() + "] >>> " + fileName);
        final AsyncHttpClient.BoundRequestBuilder builder = this.httpClient_.preparePost(url)
                .addHeader("Authorization", token.getHttpAuthzHeader());
        builder.addBodyPart(new ByteArrayPart(paramName, buffer, null, null, fileName));

        final Request request = builder.build();
        return Observable.fromEmitter(emitter -> emitJSONResponse(request, emitter), Emitter.BackpressureMode.BUFFER);


    }

    private void emitJSONResponse(final Request request, final Emitter<JSONAware> emitter) {
        final WebApiServer self = this;
        try {
            final Exception clientTrace = clientTrace();
            req_in_flight.acquire();
            final int requestNbr = self.total_req.incrementAndGet();
            //System.err.println("SENDING REQUEST:"); request.getHeaders().forEach((s, strings) -> System.err.println(s + ":" + strings));
            AsyncCompletionHandler<Response> handler = new AsyncCompletionHandler<Response>() {
                @Override
                public void onThrowable(Throwable t) {
                    req_in_flight.release();
                    t.setStackTrace(clientTrace.getStackTrace());

                    log.error("ERROR EX: {} for {}", t, request.getUrl());
                    emitter.onError(t);
                }

                @Override
                public Response onCompleted(Response response) throws Exception {
                    req_in_flight.release();
                    if (response.getStatusCode() / 100 != 2) {
                        log.error("ERROR: {} for '{}' BODY:\n{}", response.getStatusText(), request.getUrl(),
                                response.getResponseBody());
                        final RuntimeException ex = new RuntimeException("API server returned: '" + response.getStatusText() + "'");
                        ex.setStackTrace(clientTrace.getStackTrace());
                        emitter.onError(ex);
                    } else {
                        //System.out.println("----> GOT " + response.getStatusText());
                        //System.out.println(response.getResponseBody());
                        JSONParser parser = new JSONParser(JSONParser.MODE_RFC4627);
                        try {
                            JSONAware p = (JSONAware) parser.parse(response.getResponseBodyAsStream());
                            emitter.onNext(p);
                            emitter.onCompleted();
                        } catch (ParseException | UnsupportedEncodingException e) {
                            e.setStackTrace(clientTrace.getStackTrace());
                            emitter.onError(e);
                        }
                    }
                    return response;
                }
            };
            // System.err.println("[emitJSONResponse at " + Thread.currentThread().getName() + "] >>> " + requestNbr);
            ListenableFuture<Response> fut = httpClient_.executeRequest(request, handler);
            // emitter.setCancellation(() -> fut.cancel(false));

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public CompletableFuture<JSONAware> postForm(final String url,
                                                 final SAMLToken token,
                                                 final Map<String, String> formEntries,
                                                 final Map<String, ByteBuffer> formData) {
        final AsyncHttpClient.BoundRequestBuilder builder = this.httpClient_.preparePost(url)
                .addHeader("Authorization", token.getHttpAuthzHeader());

        if (formData.isEmpty()) {
            for (String n : formEntries.keySet()) {
                builder.addFormParam(n, formEntries.get(n));
            }
        } else {
            for (String n : formEntries.keySet()) {
                builder.addBodyPart(new StringPart(n, formEntries.get(n), "text/plain", StandardCharsets.UTF_8));
            }
            TimeZone tz = TimeZone.getTimeZone("UTC");
            DateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
            df.setTimeZone(tz);
            for (String n : formData.keySet()) {
                String nowFormatted = df.format(new Date());
                String filename = n + nowFormatted; // XXX: Hack, to create unique names for the inSilico Trial Repo
                builder.addBodyPart(new ByteArrayPart(n, formData.get(n).array(), null, null, filename));
            }

        }
        final Request request = builder.build();
        long contentLength = request.getContentLength();
        if (contentLength <= 0)
            request.getHeaders().put("Content-Length", Arrays.asList("0"));
        // request.getHeaders().forEach(((s, strings) -> System.out.println(s + " : " + strings.stream().collect(Collectors.joining(",")))));
        final CompletableFuture<JSONAware> fut = new CompletableFuture<>();
        final Exception clientTrace = clientTrace();
        exec.submit(() -> executeJSONRequest(request, fut, clientTrace));
        return fut;
    }

    private void executeJSONRequest(final Request request, final CompletableFuture<JSONAware> fut, final Exception clientTrace) {
        CompletableFuture<InputStream> bodyFut = new CompletableFuture<>();
        bodyFut.whenComplete((inStream, ex) -> {
            if (ex != null) {
                log.error("executing JSON Request got {}", ex.getMessage(), clientTrace);
                fut.completeExceptionally(ex);
                return;
            }
            JSONParser parser = new JSONParser(JSONParser.MODE_RFC4627);
            try {
                JSONAware p = (JSONAware) parser.parse(inStream);
                fut.complete(p);
            } catch (ParseException | UnsupportedEncodingException e) {
                e.setStackTrace(clientTrace.getStackTrace());
                fut.completeExceptionally(e);
            }
        });
        this.executeRequestAndParseResponse(request, bodyFut, clientTrace);
        /*
        try {
            req_in_flight.acquire();
            final WebApiServer self = this;
            httpClient_.executeRequest(request,
                    new AsyncCompletionHandler<Response>() {
                        @Override
                        public void onThrowable(Throwable t) {
                            req_in_flight.release();

                            System.err.println("ERROR EX: " + t + " for " + request.getUrl());
                            fut.completeExceptionally(t);
                            self.total_req.incrementAndGet();
                            self.failed_req.incrementAndGet();
                        }

                        @Override
                        public Response onCompleted(Response response) throws Exception {
                            req_in_flight.release();
                            if (response.getStatusCode() == 404)
                                fut.completeExceptionally(new RuntimeException("Resource " + response.getUri() +
                                        " (method: " + request.getMethod() + ") not found?"));
                            else if (response.getStatusCode() / 100 != 2) {
                                System.err.println("ERROR: " + response.getStatusText() + " for " + request.getUrl());
                                fut.completeExceptionally(new RuntimeException("API server returned: '" + response.getResponseBody() + "'"));
                            } else {
                                JSONParser parser = new JSONParser(JSONParser.MODE_RFC4627);
                                String js = response.getResponseBody();
                                // System.out.println("----> GOT " + response.getStatusText());
                                // System.out.println(js);
                                JSONAware p = (JSONAware) parser.parse(response.getResponseBodyAsStream());
                                fut.complete(p);
                            }
                            self.total_req.incrementAndGet();
                            return response;
                        }
                    });
        } catch (InterruptedException e) {
            e.printStackTrace();
        } */
    }

    private void executeRequestAndParseResponse(final Request request, final CompletableFuture<InputStream> fut, final Exception clientTrace) {
        try {
            req_in_flight.acquire();
            final WebApiServer self = this;
            httpClient_.executeRequest(request,
                    new AsyncCompletionHandler<Response>() {
                        @Override
                        public void onThrowable(Throwable t) {
                            req_in_flight.release();
                            t.setStackTrace(clientTrace.getStackTrace());
                            log.error("ERROR EX: {} for {}", t, request.getUrl());
                            fut.completeExceptionally(t);
                            self.total_req.incrementAndGet();
                            self.failed_req.incrementAndGet();
                        }

                        @Override
                        public Response onCompleted(Response response) throws Exception {
                            req_in_flight.release();
                            if (response.getStatusCode() == 404) {
                                final RuntimeException ex = new RuntimeException("Resource " + response.getUri() +
                                        " (method: " + request.getMethod() + ") not found?");

                                ex.setStackTrace(clientTrace.getStackTrace());
                                fut.completeExceptionally(ex);
                            }
                            else if (response.getStatusCode() / 100 != 2) {
                                log.info("ERROR EX: {} for {} ERROR:\n{}", response.getStatusText(), request.getUrl(),
                                        response.getResponseBody());
                                final RuntimeException ex = new RuntimeException("API server returned: " + response.getStatusCode());
                                ex.setStackTrace(clientTrace.getStackTrace());
                                fut.completeExceptionally(ex);
                            } else {
                                //System.out.println("----> GOT " + response.getStatusText());
                                //System.out.println(body);
                                fut.complete(response.getResponseBodyAsStream());
                            }
                            self.total_req.incrementAndGet();
                            return response;
                        }
                    });
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public final int total_requests() {
        return this.total_req.get();
    }

    public final int failed_requests() {
        return this.failed_req.get();
    }

    @Override
    public void close() throws Exception {
        this.exec.shutdown();
        this.httpClient_.close();
    }
}