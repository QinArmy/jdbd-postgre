package io.jdbd.vendor.util;

import io.jdbd.config.UrlException;
import org.qinarmy.util.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class JdbdStringUtils extends StringUtils {

    protected JdbdStringUtils() {
        throw new UnsupportedOperationException();
    }


    public static void parseQueryPair(final String originalUrl, String[] pairArray, Map<String, String> map) {
        String[] keyValue;
        String key, value;
        for (String pair : pairArray) {
            keyValue = pair.split("=");
            if (keyValue.length > 2) {
                String message = String.format("Postgre url query pair[%s] error. ", pair);
                throw new UrlException(originalUrl, message);
            }
            key = decodeQueryKeyOrValue(keyValue[0].trim());
            if (keyValue.length == 1) {
                value = "";
            } else {
                value = decodeQueryKeyOrValue(keyValue[1].trim());
            }
            map.put(key, value);
        }

    }


    protected static String decodeQueryKeyOrValue(final String keyOrValue) {
        final String decoded;
        if (!hasText(keyOrValue)) {
            decoded = keyOrValue;
        } else {
            try {
                decoded = URLDecoder.decode(keyOrValue, StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                // Won't happen.
                throw new RuntimeException(e);
            }
        }
        return decoded;
    }


}
