package gr.forth.ics.cbml.chic.hme.server;

import gr.forth.ics.cbml.chic.hme.server.utils.FileUtils;
import org.pac4j.core.client.Clients;
import org.pac4j.core.config.Config;
import org.pac4j.core.config.ConfigFactory;
import org.pac4j.saml.client.SAML2Client;
import org.pac4j.saml.client.SAML2ClientConfiguration;

/**
 * Created by ssfak on 20/1/17.
 */
public class SamlConfigFactory implements ConfigFactory {

    private final String serviceUrl;

    public SamlConfigFactory(final String serviceUrl) {
        this.serviceUrl = FileUtils.endWithSlash( serviceUrl );
    }

    public Config build() {
        final SAML2ClientConfiguration cfg = new SAML2ClientConfiguration("resource:samlKeystore.jks",
                "hme-server",
                "hme-server",
                "resource:chic_idp_metadata.xml"
        );
        cfg.setMaximumAuthenticationLifetime(2*3600);
        cfg.setServiceProviderEntityId(this.serviceUrl +"callback");
        cfg.setServiceProviderMetadataPath("hme2-metadata.xml");
        final SAML2Client saml2Client = new SAML2Client(cfg);

        final Clients clients = new Clients(this.serviceUrl + "callback?client_name=SAML2Client", saml2Client);
        clients.setDefaultClient(saml2Client);

        final Config config = new Config(clients);
        return config;
    }
}

