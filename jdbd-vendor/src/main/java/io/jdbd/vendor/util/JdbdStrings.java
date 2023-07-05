package io.jdbd.vendor.util;

import io.jdbd.JdbdException;
import io.jdbd.UrlException;
import io.jdbd.lang.Nullable;
import io.qinarmy.util.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class JdbdStrings extends StringUtils {

    protected JdbdStrings() {
        throw new UnsupportedOperationException();
    }

    public static final Pattern NUMBER_PATTERN = Pattern.compile("^-?\\d+(?:\\.\\d+(?:[eE]-?\\d+))?$");

    public static StringBuilder builder() {
        return new StringBuilder();
    }


    /**
     * @return a unmodified list
     */
    public static List<Pair<String, String>> parseStringPairList(final @Nullable String text) throws JdbdException {
        final String[] groupArray;
        if (text == null || (groupArray = text.split(",")).length == 0) {
            return Collections.emptyList();
        }

        final List<Pair<String, String>> list = JdbdCollections.arrayList(groupArray.length);
        String[] pairArray;
        for (String group : groupArray) {
            pairArray = group.split(":");
            if (pairArray.length != 2) {
                throw new JdbdException(String.format("%s format error", text));
            }
            list.add(Pair.create(pairArray[0].trim(), pairArray[1].trim()));
        }
        return JdbdCollections.unmodifiableList(list);
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

    /**
     * @throws IllegalArgumentException when enumClass not enum.
     */
    public static <T extends Enum<T>> T parseEnumValue(final Class<?> enumClass, final String textValue) {
        if (!enumClass.isEnum()) {
            throw new IllegalArgumentException(String.format("enumClass[%s] isn't enum.", enumClass));
        }
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) enumClass;
        return Enum.valueOf(clazz, textValue);
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


    /**
     * @see #bitStringToBitSet(String, boolean)
     */
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

    /**
     * @throws IllegalArgumentException when bitString isn't bit string.
     * @see #bitSetToBitString(BitSet, boolean)
     */
    public static BitSet bitStringToBitSet(final String bitString, final boolean bitEndian)
            throws IllegalArgumentException {
        final char[] bitChars = bitString.toCharArray();
        final byte[] bitBytes = new byte[bitChars.length];
        if (bitEndian) {
            char ch;
            for (int i = 0, charIndex = bitBytes.length - 1; i < bitChars.length; i++, charIndex--) {
                ch = bitChars[charIndex];
                if (ch == '1') {
                    bitBytes[i >> 3] |= (1 << (i & 7));
                } else if (ch != '0') {
                    throw new IllegalArgumentException(String.format("[%s] isn't bit string.", bitString));
                }
            }
        } else {
            char ch;
            for (int i = 0; i < bitChars.length; i++) {
                ch = bitChars[i];
                if (ch == '1') {
                    bitBytes[i >> 3] |= (1 << (i & 7));
                } else if (ch != '0') {
                    throw new IllegalArgumentException(String.format("[%s] isn't bit string.", bitString));
                }
            }
        }
        return BitSet.valueOf(bitBytes);
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

    public static String toBinaryString(final long value, final boolean bitEndian) {
        final char[] bitChars = new char[64];
        if (bitEndian) {
            long site = 1L << 63;
            for (int i = 0; i < bitChars.length; i++) {
                bitChars[i] = ((value & site) == 0) ? '0' : '1';
                site >>= 1;
            }
        } else {
            long site = 1;
            for (int i = 0; i < bitChars.length; i++) {
                bitChars[i] = ((value & site) == 0) ? '0' : '1';
                site <<= 1;
            }
        }
        return new String(bitChars);
    }

    public static String toBinaryString(final int value, final boolean bitEndian) {
        final char[] bitChars = new char[32];
        if (bitEndian) {
            int site = 1 << 31;
            for (int i = 0; i < bitChars.length; i++) {
                bitChars[i] = ((value & site) == 0) ? '0' : '1';
                site >>= 1;
            }
        } else {
            int site = 1;
            for (int i = 0; i < bitChars.length; i++) {
                bitChars[i] = ((value & site) == 0) ? '0' : '1';
                site <<= 1;
            }
        }
        return new String(bitChars);
    }

    public static String reverse(String text) {
        return new StringBuilder(text).reverse().toString();
    }


}
