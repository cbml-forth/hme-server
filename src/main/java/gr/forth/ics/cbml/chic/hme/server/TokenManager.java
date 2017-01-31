package gr.forth.ics.cbml.chic.hme.server;


import io.undertow.server.HttpServerExchange;
import io.undertow.server.session.Session;
import io.undertow.util.Sessions;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ssfak on 21/4/16.
 */
public class TokenManager {

    private final SAMLLogin samlLogin;
    private final String serviceAccount;
    private final String servicePassword;


    private final ConcurrentHashMap<String, SAMLToken> tokens = new ConcurrentHashMap<>();

    public TokenManager(final String secureTokenService, final String account_name, final String pass) {
        this.samlLogin = new SAMLLogin(secureTokenService);
        this.serviceAccount = account_name;
        this.servicePassword = pass;

    }

    private static Optional<SAMLToken> getTokenFromSession(final HttpServerExchange exchange, final String audience) {
        final Session session = Sessions.getSession(exchange);
        if (session == null)
            return Optional.empty();
        final String key = audienceAttributeName(audience);
        final Object attribute = session.getAttribute(key);
        return Optional.ofNullable(attribute).map(SAMLToken.class::cast);
    }
    private Optional<SAMLToken> getTokenFromCache(final String audience, final String actAsUser) {
        final String key = actAsUser + ":" + audience;
        final SAMLToken token = tokens.computeIfPresent(key, (k, samlToken) -> samlToken.isValid() ? samlToken : null);
        return Optional.ofNullable(token);
    }

    private static String audienceAttributeName(String audience) {
        return "token:" + audience;
    }


    public CompletableFuture<SAMLToken> getDelegationToken(final HttpServerExchange exchange, final String audience, final Optional<String> actAsUser) {

        // If a user id was given, try to find whether we have a _valid_ delegated token
        // for this user in the cache:
        final Optional<SAMLToken> tokenFromCache =
                actAsUser.flatMap(user -> getTokenFromCache(audience, user));
        if (tokenFromCache.isPresent())
            return CompletableFuture.completedFuture(tokenFromCache.get());

        // If we don't have a user id or the cache does not contain an entry
        // then try to find whether we have one in the session:
        return getTokenFromSession(exchange, audience).map(CompletableFuture::completedFuture)
                // If no token was found get a new one from STS
                .orElseGet(() -> getDelegationTokenImpl(exchange, audience, actAsUser));
    }


    private CompletableFuture<SAMLToken> getDelegationTokenImpl(final HttpServerExchange exchange, final String audience, final Optional<String> actAsUser) {
        final CompletableFuture<SAMLToken> token = samlLogin.createToken(this.serviceAccount, this.servicePassword, audience, actAsUser);
        return token.thenApply(samlToken -> {
            final Session session = Sessions.getSession(exchange);
            // Update the session:
            if (session != null) {
                final String key = audienceAttributeName(audience);
                session.setAttribute(key, samlToken);
            }
            // Update the Cache:
            else if (actAsUser.isPresent()) {
                this.tokens.put(actAsUser + ":" + audience, samlToken);
            }
            return samlToken;
        });
    }


}
