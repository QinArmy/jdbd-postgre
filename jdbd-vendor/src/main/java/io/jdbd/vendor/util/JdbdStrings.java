package io.jdbd.vendor.util;

import io.jdbd.config.UrlException;
import org.qinarmy.util.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class JdbdStrings extends StringUtils {

    protected JdbdStrings() {
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
            key = decodeUrlPart(keyValue[0].trim());
            if (keyValue.length == 1) {
                value = "";
            } else {
                value = decodeUrlPart(keyValue[1].trim());
            }
            map.put(key, value);
        }

    }


    public static String decodeUrlPart(final String urlPart) {
        final String decoded;
        if (!hasText(urlPart)) {
            decoded = urlPart;
        } else {
            try {
                decoded = URLDecoder.decode(urlPart, StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                // Won't happen.
                throw new RuntimeException(e);
            }
        }
        return decoded;
    }


}
