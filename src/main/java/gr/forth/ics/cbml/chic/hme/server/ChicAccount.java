package gr.forth.ics.cbml.chic.hme.server;

import io.undertow.predicate.Predicate;
import io.undertow.server.HttpServerExchange;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.saml.profile.SAML2Profile;
import org.pac4j.undertow.context.UndertowWebContext;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * Created by ssfak on 24/1/17.
 */
public final class ChicAccount {

    private final SAML2Profile saml2Profile;
    private final HashMap<String, String> attributes;

    private ChicAccount(SAML2Profile samlProfile) {
        this.saml2Profile = samlProfile;
        this.attributes = new HashMap<>();
        saml2Profile.getAttributes().forEach((key, value) -> {
            String strValue;
            if (value instanceof List) {
                List l = (List) value;
                if (l.size() == 1)
                    strValue = l.get(0).toString();
                else
                    strValue = value.toString();
            } else
                strValue = value.toString();
            attributes.put(samlNamesToUserFriendly.getOrDefault(key, key), strValue);
        });
    }


    static Optional<ChicAccount> currentUser(final HttpServerExchange exchange) {

        UndertowWebContext context = new UndertowWebContext(exchange);
        ProfileManager manager = new ProfileManager(context);

        if (!manager.isAuthenticated())
            return Optional.empty();

        Optional<CommonProfile> profile = manager.get(true);

        return profile.map(SAML2Profile.class::cast).map(ChicAccount::new);
    }

    @Override
    public String toString() {
        return "<User " + this.getUserId() + " [" + this.getUserName() + ", " + this.getEmail() + "]>";
    }

    private static HashMap<String, String> samlNamesToUserFriendly =
            new HashMap<String, String>() {
                {
                    put("urn:oid:0.9.2342.19200300.100.1.1", "rfc2798_uid");
                    put("urn:custodix:ciam:1.0:principal:uuid", "uid");
                    put("urn:custodix:ciam:1.0:domain:name", "domain");
                    put("urn:custodix:ciam:1.0:domain:uuid", "domainUuid");
                    put("urn:oid:0.9.2342.19200300.100.1.3", "mail");
                    put("urn:oid:2.16.840.1.113730.3.1.241", "displayName");
                    put("urn:oid:2.5.4.3", "cn");
                    put("urn:oid:2.5.4.4", "sn");
                    put("urn:oid:2.5.4.12", "title");
                    put("urn:oid:2.5.4.20", "phone");
                    put("urn:oid:2.5.4.42", "givenName");
                    put("urn:oid:2.5.6.8", "organizationalRole");
                    put("urn:oid:2.16.840.1.113730.3.1.3", "employeeNumber");
                    put("urn:oid:2.16.840.1.113730.3.1.4", "employeeType");
                    put("urn:oid:1.3.6.1.4.1.5923.1.1.1.1", "eduPersonAffiliation");
                    put("urn:oid:1.3.6.1.4.1.5923.1.1.1.2", "eduPersonNickname");
                    put("urn:oid:1.3.6.1.4.1.5923.1.1.1.6", "eduPersonPrincipalName");
                    put("urn:oid:1.3.6.1.4.1.5923.1.1.1.9", "eduPersonScopedAffiliation");
                    put("urn:oid:1.3.6.1.4.1.5923.1.1.1.10", "eduPersonTargetedID");
                    put("urn:oid:1.3.6.1.4.1.5923.1.6.1.1", "eduCourseOffering");
                }
            };

    String getUserId() {
        if (attributes.containsKey("uid"))
            return attributes.get("uid");
        return attributes.getOrDefault("rfc2798_uid", "");
    }

    String getUserName() {
        if (attributes.containsKey("displayName"))
            return attributes.get("displayName");
        if (attributes.containsKey("cn"))
            return attributes.get("cn");
        if (attributes.containsKey("givenName") && attributes.containsKey("sn"))
            return attributes.get("givenName") + " " + attributes.get("sn");
        return "";
    }

    String getEmail() {
        return attributes.getOrDefault("mail", "");
    }

    static boolean isAuthenticated(HttpServerExchange exchange) {
        return currentUser(exchange).isPresent();
    }

    static Predicate authnPredicate() {
        return ChicAccount::isAuthenticated;
    }
}
