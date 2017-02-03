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

import lombok.extern.slf4j.Slf4j;
import nu.xom.*;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Created by ssfak on 14/12/15.
 */
@Slf4j
public class SAMLToken {
    private Element xml;
    private Map<String, String> assertions;

    private String httpSamlToken;

    public Instant getExpirationDate() {
        return expirationDate;
    }

    private Instant expirationDate;

    public static final XPathContext xPathContext = new XPathContext();

    static {
        xPathContext.addNamespace("w", "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd");
        xPathContext.addNamespace("wsp", "http://schemas.xmlsoap.org/ws/2004/09/policy");
        xPathContext.addNamespace("wsa", "http://www.w3.org/2005/08/addressing");
        xPathContext.addNamespace("wst", "http://docs.oasis-open.org/ws-sx/ws-trust/200512");
        xPathContext.addNamespace("saml", "urn:oasis:names:tc:SAML:2.0:assertion");
    }

    private SAMLToken() {}

    public static Optional<SAMLToken> fromXml(InputStream xmlStream) {
        try {
            Element element = new Builder().build(xmlStream).getRootElement();
            return fromXml(element);
        }
        catch (Exception e) {
            log.error("XML parse", e);
        }
        return Optional.empty();
    }

    public static Optional<SAMLToken> fromXml(Element xml) {
        final SAMLToken token = new SAMLToken();
        token.parse_assertions(xml);
        if (token.assertions.isEmpty())
            return Optional.empty();
        token.xml = xml;
        token.create_http_token();
        return Optional.of(token);
    }

    public static Optional<SAMLToken> fromHttpAuthzHeader(final String header) {
        String token = header;

        final String prefix = "SAML auth=";
        if (header.startsWith(prefix))
            token = header.substring(prefix.length());

        try {
            final byte[] bytes = token.getBytes(StandardCharsets.ISO_8859_1);
            final Base64.Decoder decoder = Base64.getDecoder();
            final byte[] data = decoder.decode(bytes);
            final Inflater unZipper = new Inflater();
            unZipper.setInput(data);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
            byte[] buffer = new byte[1024];
            while (!unZipper.finished()) {
                int count = unZipper.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();
            unZipper.end();

            byte[] output = outputStream.toByteArray();

            Element element = new Builder().build(new ByteArrayInputStream(output)).getRootElement();
            return fromXml(element);

        } catch (Exception e) {
            log.error("Token parse", e);
        }
        return Optional.empty();
    }

    private void create_http_token() {
        final Nodes nodes = this.xml.query("//saml:Assertion", xPathContext);
        if (nodes.size() == 0) {
            // Well, this should not happen if the login was successfull
            this.httpSamlToken = ""; // XXX
            return;
        }
        final Element token_xml = (Element) nodes.get(0);

        // System.out.println(token_xml.toXML());
        final byte[] bytes = token_xml.toXML().getBytes(StandardCharsets.UTF_8);
        Deflater zipper = new Deflater();
        zipper.setInput(bytes);
        zipper.finish();
        final byte[] output_bytes = new byte[bytes.length];
        int len = zipper.deflate(output_bytes);
        zipper.end();
        final Base64.Encoder encoder = Base64.getEncoder();
        final ByteBuffer b64_bytes = encoder.encode(ByteBuffer.wrap(output_bytes, 0, len));

        // System.out.println(len + " -> " + b64_bytes.array().length);
        this.httpSamlToken = new String(b64_bytes.array(), StandardCharsets.ISO_8859_1);
    }

    public Map<String, String> getAssertions() {
        return Collections.unmodifiableMap(assertions);
    }

    public boolean isValid() {
        return Instant.now().isBefore(this.getExpirationDate());
    }
    public String getHttpSamlToken() {
        return httpSamlToken;
    }

    public String getHttpAuthzHeader() {
        return "SAML auth=" + httpSamlToken;
    }

    private void parse_assertions(Element xml) {
        final Nodes nodes = xml.query("//saml:Assertion", xPathContext);

        Map<String, String> assertions = new HashMap<String, String>();
        for (int i = 0; i < nodes.size(); ++i) {
            final Element assertion = (Element) nodes.get(i);
            final Nodes nodes1 = assertion.query("//saml:Attribute", xPathContext);
            for (int j = 0; j < nodes1.size(); j++) {
                final Element element1 = (Element) nodes1.get(j);
                final String name = element1.getAttribute("Name").getValue();
                final Elements attrValues = element1.getChildElements("AttributeValue", xPathContext.lookup("saml"));
                List<String> values = new ArrayList<>(attrValues.size());
                for (int k = 0; k < attrValues.size(); k++) {
                    values.add(attrValues.get(k).getValue());
                }
                final String value = String.join(",", values); // XXX: what to do with multiple values?
                assertions.put(name, value);
            }

            final Nodes dateConditions = assertion.query("//saml:Conditions[@NotOnOrAfter]", xPathContext);
            for (int j = 0; j < dateConditions.size(); j++) {
                final Element condition = (Element) dateConditions.get(i);
                final Instant notOnOrAfter = DatatypeConverter.parseDateTime(condition.getAttribute("NotOnOrAfter").getValue()).toInstant();
                if (this.expirationDate == null || this.expirationDate.isAfter(notOnOrAfter))
                    this.expirationDate = notOnOrAfter;
            }
        }
        this.assertions = assertions;
    }

    public String getUserId() {
        if (assertions.containsKey("urn:custodix:ciam:1.0:principal:uuid"))
            return assertions.get("urn:custodix:ciam:1.0:principal:uuid");
        return assertions.getOrDefault("urn:oid:0.9.2342.19200300.100.1.1", "");
    }
}
