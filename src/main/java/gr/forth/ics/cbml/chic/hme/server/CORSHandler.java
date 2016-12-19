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


import io.undertow.io.IoCallback;
import io.undertow.server.HandlerWrapper;
import io.undertow.server.HttpHandler;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.StatusCodes;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by ssfak on 5/8/16.
 */
public class CORSHandler implements HandlerWrapper {
    private static List<String> allowedHeaders = Arrays.asList("LOCATION", "ETAG", "X-HME-SID");
    private static Set<String> allowedMethods = new HashSet<>(Arrays.asList("GET", "POST", "OPTIONS"));

    @Override
    public HttpHandler wrap(HttpHandler httpHandler) {

        HttpHandler h = (exchange) -> {
            final HttpString requestMethod = exchange.getRequestMethod();
            final HeaderMap requestHeaders = exchange.getRequestHeaders();

            // See http://www.html5rocks.com/static/images/cors_server_flowchart.png

            boolean isPreflight = requestHeaders.contains(Headers.ORIGIN)
                    && requestMethod.equalToString("OPTIONS")
                    && requestHeaders.contains(HttpString.tryFromString("Access-Control-Request-Method"));
            if (isPreflight) {
                final String req_method = requestHeaders.get("Access-Control-Request-Method").getFirst();
                if (!allowedMethods.contains(req_method)) {
                    exchange.setStatusCode(StatusCodes.UNAUTHORIZED);
                    exchange.getResponseSender().send("Not valid CORS preflight request..", IoCallback.END_EXCHANGE);
                    return;
                }
                exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Methods"), String.join(",", allowedMethods));
                exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Headers"), String.join(",", allowedHeaders));
            } else {
                exchange.getResponseHeaders().put(new HttpString("Access-Control-Expose-Headers"), String.join(",", allowedHeaders));
            }

            exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Origin"), "*");
            exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Credentials"), "true");
            if (isPreflight) {
                exchange.setStatusCode(StatusCodes.OK);
                exchange.endExchange();
                return;
            }
            httpHandler.handleRequest(exchange);
        };
        return h;
    }

}
