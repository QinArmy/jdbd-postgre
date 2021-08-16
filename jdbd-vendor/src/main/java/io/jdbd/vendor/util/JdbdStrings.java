package io.jdbd.vendor.util;

import io.jdbd.config.UrlException;
import org.qinarmy.util.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class JdbdStrings extends StringUtils {

    protected JdbdStrings() {
        throw new UnsupportedOperationException();
    }

    public static final Pattern NUMBER_PATTERN = Pattern.compile("^-?\\d+(?:\\.\\d+(?:[eE]-?\\d+))?$");


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

    public static boolean isNumber(final String text) {
        return NUMBER_PATTERN.matcher(text).matches();
    }

    public static String bitSetToBitString(BitSet bitSet, final boolean bitEndian) {
        final byte[] bitBytes = bitSet.toByteArray();
        final int length = bitSet.length();
        final char[] bitChars = new char[length];
        if (bitEndian) {
            for (int i = 0, bitIndex = length - 1; i < length; i++, bitIndex--) {
                bitChars[i] = (bitBytes[bitIndex >> 3] & (1 << (bitIndex & 7))) == 0 ? '0' : '1';
            }
        } else {
            for (int i = 0; i < length; i++) {
                bitChars[i] = (bitBytes[i >> 3] & (1 << (i & 7))) == 0 ? '0' : '1';
            }
        }
        return new String(bitChars);
    }

    public static boolean isBinaryString(String text) {
        final char[] charArray = text.toCharArray();
        boolean match = charArray.length > 0;
        for (char c : charArray) {
            if (c == '0' || c == '1') {
                continue;
            }
            match = false;
            break;
        }
        return match;
    }

    public static String reverse(String text) {
        final char[] charArray = text.toCharArray();
        char temp;
        for (int left = 0, right = charArray.length - 1, end = charArray.length >> 1; left < end; left++, right--) {
            temp = charArray[left];
            charArray[left] = charArray[right];
            charArray[right] = temp;
        }
        return new String(charArray);
    }


}
