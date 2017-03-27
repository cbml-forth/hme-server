package gr.forth.ics.cbml.chic.hme.server;


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by ssfak on 21/4/16.
 */
@Slf4j
public class TokenManager {

    private final SAMLLogin samlLogin;
    private final String serviceAccount;
    private final String servicePassword;


    private final Cache<String, SAMLToken> tokens;

    public TokenManager(final String secureTokenService, final String account_name, final String pass) {
        this.samlLogin = new SAMLLogin(secureTokenService);
        this.serviceAccount = account_name;
        this.servicePassword = pass;

        log.info("Initializing.. STS={}, account={}", secureTokenService, account_name);
        tokens = Caffeine.newBuilder()
                .maximumSize(4 * 1_000)
                .expireAfterWrite(2, TimeUnit.HOURS)
                .removalListener((key, samlToken, removalCause) -> System.err.println("* CACHE : '" + key + "' was removed because of " + removalCause+ " time=" + Instant.now()))
                .build();

    }

    private Optional<SAMLToken> getTokenFromCache(final String audience, final String actAsUser) {
        final String key = cacheKey(actAsUser, audience);
        return Optional.ofNullable(tokens.getIfPresent(key)).filter(SAMLToken::isValid);
    }

    private static String cacheKey(String user, String audience) {
        return user.hashCode() + ":" + audience;
    }


    public CompletableFuture<SAMLToken> getDelegationToken(final URI audience, final String actAsUser) {
        return this.getDelegationToken(audience.toString(), actAsUser);
    }
    public CompletableFuture<SAMLToken> getDelegationToken(final String audience, final String actAsUser) {

        // Try to find whether we have a _valid_ delegated token
        // for this user in the cache
        // If the cache does not contain an entry for this User
        // then create a new one (and add it to the cache):
        return getTokenFromCache(audience, actAsUser)
                .map(CompletableFuture::completedFuture)
                .orElseGet(()->getDelegationTokenImpl(audience, actAsUser));
    }


    private CompletableFuture<SAMLToken> getDelegationTokenImpl(final String audience, final String actAsUser) {
        return this.samlLogin
                .createDelegatedToken(this.serviceAccount, this.servicePassword, audience, actAsUser)
                .whenComplete((samlToken, throwable) -> {
                    if (throwable == null) {
                        // Update the Cache:
                        log.info("Got delegation token for user {} to access {}", actAsUser, audience);
                        this.tokens.put(cacheKey(actAsUser, audience), samlToken);
                    }
                });
    }
}
