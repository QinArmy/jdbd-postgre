package io.jdbd.vendor.util;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.qinarmy.util.CollectionUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

public abstract class JdbdStrings /*extends StringUtils*/ {

    protected JdbdStrings() {
        throw new UnsupportedOperationException();
    }

    public static final Pattern NUMBER_PATTERN = Pattern.compile("^-?\\d+(?:\\.\\d+(?:[eE]-?\\d+))?$");

    public static StringBuilder builder() {
        return new StringBuilder();
    }

    public static boolean hasText(final @Nullable CharSequence str) {
        final int strLen;

        if (str == null || (strLen = str.length()) == 0) {
            return false;
        }
        boolean match = false;
        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(str.charAt(i))) {
                continue;
            }
            match = true;
            break;
        }
        return match;
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


    /**
     * @return a modifiable list
     */
    public static List<String> spitAsList(@Nullable String text, String regex) {
        return spitAsList(text, regex, false);
    }

    public static List<String> spitAsList(@Nullable String text, String regex, final boolean unmodifiable) {
        final List<String> list;
        if (hasText(text)) {
            String[] itemArray = text.split(regex);
            if (itemArray.length == 1 && unmodifiable) {
                list = Collections.singletonList(itemArray[0].trim());
            } else {
                List<String> tempList = new ArrayList<>(itemArray.length);
                for (String p : itemArray) {
                    tempList.add(p.trim());
                }
                list = unmodifiable ? Collections.unmodifiableList(tempList) : tempList;
            }
        } else if (unmodifiable) {
            list = Collections.emptyList();
        } else {
            list = new ArrayList<>();
        }
        return list;
    }

    /**
     * @return a modifiable list
     */
    public static Set<String> spitAsSet(@Nullable String text, String regex) {
        final Set<String> set;
        if (hasText(text)) {
            String[] itemArray = text.split(regex);
            set = new HashSet<>((int) (itemArray.length / 0.75F));
            for (String p : itemArray) {
                set.add(p.trim());
            }
        } else {
            set = new HashSet<>();
        }
        return set;
    }

    public static Set<String> spitAsSet(@Nullable String text, String regex, final boolean unmodifiable) {
        final Set<String> set;
        set = spitAsSet(text, regex);
        return unmodifiable ? CollectionUtils.unmodifiableSet(set) : set;
    }

    public static Map<String, String> spitAsMap(final @Nullable String text, final String regex1,
                                                final String regex2, final boolean unmodifiable) {
        Map<String, String> map;
        if (!hasText(text)) {
            if (unmodifiable) {
                map = Collections.emptyMap();
            } else {
                map = JdbdCollections.hashMap();
            }
            return map;
        }
        final String[] pairArray = text.split(regex1);
        map = JdbdCollections.hashMap((int) (pairArray.length / 0.75f));
        String[] kv;
        for (String pair : pairArray) {
            kv = pair.split(regex2);
            if (kv.length != 2) {
                throw new IllegalStateException(String.format("%s can't resolve pair.", text));
            }
            map.put(kv[0].trim(), kv[1].trim());
        }

        if (unmodifiable) {
            map = JdbdCollections.unmodifiableMap(map);
        }
        return map;
    }

    public static String concat(List<String> list, String delimiter) {
        StringBuilder builder = new StringBuilder();
        final int size = list.size();
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                builder.append(delimiter);
            }
            builder.append(list.get(i));
        }
        return builder.toString();
    }


    public static void parseQueryPair(final String originalUrl, String[] pairArray, Map<String, String> map) {
        String[] keyValue;
        String key, value;
        for (String pair : pairArray) {
            keyValue = pair.split("=");
            if (keyValue.length > 2) {
                String message = String.format("Postgre url query pair[%s] error. ", pair);
                throw new JdbdException(message);
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
    public static String bitSetToBitString(final BitSet bitSet, final boolean bitEndian) {
        final int length = bitSet.length();
        final char[] bitChars = new char[length];
        if (bitEndian) {
            for (int i = 0, bitIndex = length - 1; i < length; i++, bitIndex--) {
                if (bitSet.get(bitIndex)) {
                    bitChars[i] = '1';
                } else {
                    bitChars[i] = '0';
                }
            }
        } else {
            for (int i = 0; i < length; i++) {
                if (bitSet.get(i)) {
                    bitChars[i] = '1';
                } else {
                    bitChars[i] = '0';
                }
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
        final int length;
        length = bitString.length();
        final BitSet bitSet = new BitSet(length);
        char ch;
        if (bitEndian) {
            for (int i = 0, bitIndex = length - 1; i < length; i++, bitIndex--) {
                ch = bitString.charAt(i);
                if (ch == '1') {
                    bitSet.set(bitIndex, true);
                } else if (ch != '0') {
                    throw new IllegalArgumentException(String.format("[%s] isn't bit string.", bitString));
                }
            }
        } else {
            for (int i = 0; i < length; i++) {
                ch = bitString.charAt(i);
                if (ch == '1') {
                    bitSet.set(i, true);
                } else if (ch != '0') {
                    throw new IllegalArgumentException(String.format("[%s] isn't bit string.", bitString));
                }
            }
        }
        return bitSet;
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
