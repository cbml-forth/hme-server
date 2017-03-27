package gr.forth.ics.cbml.chic.hme.server;

import io.undertow.predicate.Predicate;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.saml.profile.SAML2Profile;
import org.pac4j.undertow.context.UndertowWebContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by ssfak on 24/1/17.
 */
public final class ChicAccount {

    private final Map<String, String> attributes;

    private ChicAccount(final Map<String, String> attrs)
    {
        this.attributes = new HashMap<>();
        attrs.forEach((key, value) -> this.attributes.put(samlNamesToUserFriendly.getOrDefault(key, key), value));
    }
    private static ChicAccount buildFromProfile(SAML2Profile saml2Profile) {
        Map<String, String> attributes = new HashMap<>();
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
            attributes.put(key, strValue);
        });
        return new ChicAccount(attributes);
    }

    static Optional<ChicAccount> currentUser(final HttpServerExchange exchange) {
        UndertowWebContext context = new UndertowWebContext(exchange);
        ProfileManager manager = new ProfileManager(context);

        if (manager.isAuthenticated()) {
            // This covers the case of Cookie based authentication used by the UI:
            final Optional<CommonProfile> profile = manager.get(true);
            return profile.map(SAML2Profile.class::cast).map(ChicAccount::buildFromProfile);
        }

        if (exchange.getRequestHeaders().contains(Headers.AUTHORIZATION)) {
            // check if there's an Authorization header with the SAML token
            // This could be the case of another component using the REST API
            return SAMLToken.fromHttpAuthzHeader(exchange.getRequestHeaders().getFirst(Headers.AUTHORIZATION))
                    .filter(SAMLToken::isValid)
                    .map(SAMLToken::getAssertions)
                    .map(ChicAccount::new);
        }
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "<User " + this.getUserId() + " [" + this.getUsername() + ", " + this.getEmail() + "]>";
    }

    private static HashMap<String, String> samlNamesToUserFriendly =
            new HashMap<String, String>() {
                {
                    put("urn:oid:0.9.2342.19200300.100.1.1", "rfc2798_uid");
                    put("urn:custodix:ciam:1.0:principal:uuid", "uid");
                    put("urn:custodix:ciam:1.0:domain:name", "domain");
                    put("urn:custodix:ciam:1.0:domain:uuid", "domainUuid");
                    put("urn:oid:0.9.2342.19200300.100.1.3", "email");
                    put("urn:custodix:ciam:1.0:principal:email", "email");
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
    String getUsername() {
        return attributes.getOrDefault("rfc2798_uid", "");
    }

    String getPersonName() {
        if (attributes.containsKey("displayName"))
            return attributes.get("displayName");
        if (attributes.containsKey("cn"))
            return attributes.get("cn");
        if (attributes.containsKey("givenName") && attributes.containsKey("sn"))
            return attributes.get("givenName") + " " + attributes.get("sn");
        return "";
    }

    String getEmail() {
        return attributes.getOrDefault("email", "");
    }

    String attrsToString() {
        return this.attributes.entrySet().stream()
                .map(entry-> entry.getKey() + " = " + entry.getValue())
                .collect(Collectors.joining("\n"));
    }


    static boolean isAuthenticated(HttpServerExchange exchange) {
        return currentUser(exchange).isPresent();
    }

    static Predicate authnPredicate() {
        return ChicAccount::isAuthenticated;
    }
}
