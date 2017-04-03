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

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Config;

import java.net.URI;

/**
 * Created by ssfak on 7/4/16.
 */
@HmeServerConfig.Sources({"classpath:HmeServerConfig.properties" })
public interface HmeServerConfig extends Config, Accessible {

    @DefaultValue("9090")
    int port();

    @DefaultValue("0.0.0.0")
    String hostname();

    String secureTokenService();

    String staticDir();

    // The credentials used for the CRAF Service Account
    @DefaultValue("hme")
    String serviceAccountName();
    String serviceAccountPassword();

    String sparqlRicordo();

    @DefaultValue("https://hme.chic-vph.eu/hme")
    String serviceUrl();

    @DefaultValue("https://cdr.chic-vph.eu/")
    URI cdrServiceUrl();

    @DefaultValue("https://hf.chic-vph.eu/api/")
    URI hfServiceUrl();

    @DefaultValue("https://mr.chic-vph.eu/model_app/")
    URI mrServiceUrl();

    @DefaultValue("https://istr.chic-vph.eu/api/")
    URI istrServiceUrl();

    @DefaultValue("60")
    int maxSessionInactivity();

    @DefaultValue("1000")
    int maxSessions();

    @DefaultValue("hme")
    String dbUser();
    @DefaultValue("hme")
    String dbPwd();

    @DefaultValue("localhost")
    String dbHost();

    @DefaultValue("hme")
    String dbName();

    @Key("rabbimq.host")
    @DefaultValue("127.0.0.1")
    String amqpHost();

    @Key("rabbimq.port")
    @DefaultValue("5672")
    int amqpPort();

    @Key("rabbimq.user")
    @DefaultValue("chic")
    String amqpUser();

    @Key("rabbimq.password")
    @DefaultValue("chic")
    String amqpPassword();

    @Key("rabbimq.vhost")
    @DefaultValue("chic")
    String amqpVirtualHost();

    @Key("rabbimq.threads")
    @DefaultValue("3")
    int amqpThreadsNbr();


    @Key("keystore.path")
    String keystorePath();

    @Key("keystore.password")
    String keystorePassword();

    @Key("keystore.privateKeyPassword")
    String privateKeyPassword();

    @Key("identityProviderMetadataPath")
    String identityProviderMetadataPath();

}
