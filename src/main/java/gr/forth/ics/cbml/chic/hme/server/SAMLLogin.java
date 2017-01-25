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
import gr.forth.ics.cbml.chic.hme.server.utils.FileUtils;
import nu.xom.Builder;
import nu.xom.Element;
import nu.xom.ParsingException;
import nu.xom.XPathContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by ssfak on 19/11/15.
 */
public class SAMLLogin implements AutoCloseable {


    final String secureTokenService;

    public final AsyncHttpClient httpClient;

    public final String samlReqTemplate;


    public static final XPathContext xPathContext = new XPathContext();

    static {
        xPathContext.addNamespace("wsse", "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd");
        xPathContext.addNamespace("wsp", "http://schemas.xmlsoap.org/ws/2004/09/policy");
        xPathContext.addNamespace("wsa", "http://www.w3.org/2005/08/addressing");
        xPathContext.addNamespace("wst", "http://docs.oasis-open.org/ws-sx/ws-trust/200512");
        xPathContext.addNamespace("saml", "urn:oasis:names:tc:SAML:2.0:assertion");
        xPathContext.addNamespace("soap", "http://schemas.xmlsoap.org/soap/envelope/");
        xPathContext.addNamespace("wst14", "http://docs.oasis-open.org/ws-sx/ws-trust/200802");
    }

    public SAMLLogin(final String secureTokenService) {
        this(new AsyncHttpClient(new AsyncHttpClientConfig.Builder()
                        .setAllowPoolingConnections(true)
                        .setAcceptAnyCertificate(true)
                        .build()),
                secureTokenService);
    }

    public SAMLLogin(final AsyncHttpClient httpClient, final String secureTokenService) {
        this.httpClient = httpClient;
        this.secureTokenService = secureTokenService;

        this.samlReqTemplate = readAllLines("resource:saml_req.xml");

    }

    private static String readAllLines(final String fname) {
        String lines = "";
        try (InputStream ins = FileUtils.getInputStreamFromName(fname)) {
            lines = FileUtils.readAllChars(ins);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    private String create_token_request(final String username, final String password, final String audience, Optional<String> actAsUser)
            throws ParsingException, IOException {
        Builder b = new Builder();

        final String template = this.samlReqTemplate;
        Element element = b.build(new StringReader(template)).getRootElement();
        final Element toElement = (Element) element.query("//soap:Header/wsa:To", xPathContext).get(0);
        toElement.removeChildren();
        toElement.insertChild(secureTokenService, 0);

        final Element msgElement = (Element) element.query("//soap:Header/wsa:MessageID", xPathContext).get(0);
        msgElement.removeChildren();
        String msgId = UUID.randomUUID().toString();
        msgElement.insertChild("urn:uuid:"+msgId, 0);

        final Element securityHeader = (Element) element.query("//soap:Header/wsse:Security", xPathContext).get(0);

        final Element usernameElement = (Element) securityHeader.query("wsse:UsernameToken/wsse:Username", xPathContext).get(0);
        usernameElement.insertChild(username, 0);
        final Element passwordElement = (Element) securityHeader.query("wsse:UsernameToken/wsse:Password", xPathContext).get(0);
        passwordElement.insertChild(password, 0);
        final Element requestToken = (Element) element.query("//soap:Body/wst:RequestSecurityToken", xPathContext).get(0);
        final Element serviceElement = (Element) requestToken.query("wsp:AppliesTo/wsa:EndpointReference/wsa:Address", xPathContext).get(0);
        serviceElement.insertChild(audience, 0);

        actAsUser.ifPresent(userToImpersonate -> {
            final Element usernameElement2 = new Element("Username", xPathContext.lookup("wsse"));
            usernameElement2.insertChild(userToImpersonate, 0);

            final Element usernameTokenElement = new Element("UsernameToken", xPathContext.lookup("wsse"));
            usernameTokenElement.insertChild(usernameElement2, 0);
            final Element actAsElement = new Element("ActAs", xPathContext.lookup("wst14"));
            actAsElement.insertChild(usernameTokenElement, 0);
            requestToken.appendChild(actAsElement);
        });

        return element.toXML();
    }


    public CompletableFuture<SAMLToken> createToken(final String username, final String password, final String audience, Optional<String> actAsUser) {

        CompletableFuture<SAMLToken> fut = new CompletableFuture<>();
        try {
            String xml_request = create_token_request(username, password, audience, actAsUser);
            // System.err.println(xml_request);

            final Request request =
                    this.httpClient.preparePost(secureTokenService).addHeader("Content-type", "application/xml")
                            .setBody(xml_request).build();


            this.httpClient.executeRequest(request,
                    new AsyncCompletionHandler<Response>() {
                        @Override
                        public void onThrowable(Throwable t) {
                            System.err.println("ERROR EX: " + t + " for " + request.getUrl());
                            fut.completeExceptionally(t);
                        }

                        @Override
                        public Response onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() == 404)
                                fut.completeExceptionally(new RuntimeException("Resource " + response.getUri() +
                                        " (method: " + request.getMethod() + ") not found?"));
                            else if (response.getStatusCode() / 100 != 2) {
                                System.err.println("ERROR: " + response.getStatusText() + " for " + request.getUrl() + " ERROR: " + response.getResponseBody());
                                fut.completeExceptionally(new RuntimeException("API server returned: '" + response.getResponseBody() + "'"));
                            } else {
                                final SAMLToken samlToken = SAMLToken.fromXml(response.getResponseBodyAsStream())
                                        .orElseGet(() -> null);
                                fut.complete(samlToken);
                            }
                            return response;
                        }
                    });

        } catch (Exception e) {
            e.printStackTrace();
            fut.completeExceptionally(e);
        }
        return fut;
    }

    @Override
    public void close() {
        this.httpClient.close();
    }
}
