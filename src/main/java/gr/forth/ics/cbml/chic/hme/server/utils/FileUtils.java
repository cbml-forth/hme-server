package gr.forth.ics.cbml.chic.hme.server.utils;

import java.io.*;

/**
 * Created by ssfak on 20/1/17.
 */
public final class FileUtils {

    public static final String RESOURCE_PREFIX = "resource";
    public static final String CLASSPATH_PREFIX = "classpath";

    /**
     * Extract the prefix of the name.
     *
     * @param name the name
     * @return the prefix
     */
    private static String extractPrefix(final String name) {
        String prefix = null;
        if (name != null) {
            int prefixEnd = name.indexOf(":");
            if (prefixEnd != -1) {
                prefix = name.substring(0, prefixEnd);
            }
        }
        return prefix;
    }

    /**
     * Add a slash at the beginning of a path if missing.
     *
     * @param path the path
     * @return the completed path
     */
    private static String startWithSlash(final String path) {
        if (!path.startsWith("/")) {
            return "/" + path;
        } else {
            return path;
        }
    }
    /**
     * Returns an {@link InputStream} from given name depending on its format:
     * - loads from the classloader of this class if name starts with "resource:" (add a slash a the beginning if absent)
     * - loads from the classloader of the current thread if name starts with "classpath:"
     * - loads as {@link FileInputStream} otherwise
     * <p>
     * Caller is responsible for closing inputstream
     *
     * @param name name of the resource
     * @return the input stream
     */
    public static InputStream getInputStreamFromName(String name) {
        String path = name;
        final String prefix = extractPrefix(name);
        if (prefix != null) {
            path = name.substring(prefix.length() + 1);
        }
        if (prefix == null || prefix.isEmpty()) {
            try {
                return new FileInputStream(path);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        switch (prefix) {
            case RESOURCE_PREFIX:
                return FileUtils.class.getResourceAsStream(startWithSlash(path));
            case CLASSPATH_PREFIX:
                return Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
            default:
                throw new RuntimeException("prefix is not handled:" + prefix);
        }

    }


    public static String readAllChars(final InputStream ins) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = ins.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString("UTF-8");
    }
}
