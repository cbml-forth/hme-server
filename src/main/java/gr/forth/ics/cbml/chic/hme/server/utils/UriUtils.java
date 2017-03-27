package gr.forth.ics.cbml.chic.hme.server.utils;

import lombok.experimental.UtilityClass;

import java.net.URI;

/**
 * Created by ssfak on 14/3/17.
 */
@UtilityClass
public class UriUtils {

    public static URI baseURI(final URI serviceUri) {
        return URI.create(serviceUri + "/").normalize();
    }
    public static URI audienceURI(final URI serviceUri)
    {
        final URI baseUri = baseURI(serviceUri);
        return baseUri.resolve("/");
    }
}
