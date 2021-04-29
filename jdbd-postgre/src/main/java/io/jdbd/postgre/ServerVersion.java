package io.jdbd.postgre;

import reactor.util.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ServerVersion implements Comparable<ServerVersion> {


    public static final ServerVersion INVALID = ServerVersion.parse("0.0.0");
    public static final ServerVersion V8_2 = ServerVersion.parse("8.2.0");
    public static final ServerVersion V8_3 = ServerVersion.parse("8.3.0");
    public static final ServerVersion V8_4 = ServerVersion.parse("8.4.0");

    public static final ServerVersion V9_0 = ServerVersion.parse("9.0.0");
    public static final ServerVersion V9_1 = ServerVersion.parse("9.1.0");
    public static final ServerVersion V9_2 = ServerVersion.parse("9.2.0");
    public static final ServerVersion V9_3 = ServerVersion.parse("9.3.0");

    public static final ServerVersion V9_4 = ServerVersion.parse("9.4.0");
    public static final ServerVersion V9_5 = ServerVersion.parse("9.5.0");
    public static final ServerVersion V9_6 = ServerVersion.parse("9.6.0");
    public static final ServerVersion V10 = ServerVersion.parse("10");

    public static final ServerVersion V11 = ServerVersion.parse("11");
    public static final ServerVersion V12 = ServerVersion.parse("12");


    static final List<ServerVersion> CACHE_VERSION_LIST;

    static {
        CACHE_VERSION_LIST = createServerVersionList();
    }


    private final String completeVersion;

    private final int version;

    private ServerVersion(String completeVersion, int version) {
        this.completeVersion = completeVersion;
        this.version = version;
    }


    public final String getCompleteVersion() {
        return this.completeVersion;
    }

    public final int getVersion() {
        return this.version;
    }

    public final int getMajor() {
        return this.version / 10000;
    }

    public final int getMinor() {
        return (this.version % 10000) / 100;
    }

    public final int getSubMinor() {
        return this.version % 100;
    }


    @Override
    public final int compareTo(ServerVersion o) {
        return this.version - o.version;
    }

    @Override
    public final boolean equals(@Nullable Object nullable) {
        final boolean match;
        if (nullable == this) {
            match = true;
        } else if (nullable instanceof ServerVersion) {
            ServerVersion v = (ServerVersion) nullable;
            return v.version == this.version;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final int hashCode() {
        return this.version;
    }

    @Override
    public String toString() {
        return new StringBuilder("ServerVersion{")
                .append("completeVersion=")
                .append(this.completeVersion)
                .append(",version=")
                .append(this.version)
                .append(",major=")
                .append(getMajor())
                .append(",minor=")
                .append(getMinor())
                .append(",subMinor=")
                .append(getSubMinor())
                .append("}")
                .toString();
    }

    /**
     * @throws IllegalArgumentException throw when completeVersion error.
     */
    public static ServerVersion from(final String completeVersion) {
        for (ServerVersion serverVersion : CACHE_VERSION_LIST) {
            if (serverVersion.completeVersion.equals(completeVersion)) {
                return serverVersion;
            }
        }
        return parse(completeVersion);
    }

    private static ServerVersion parse(final String completeVersion) {
        String[] array = completeVersion.split("\\.");
        if (array.length > 3) {
            String message = String.format("%s error.", completeVersion);
            throw new IllegalArgumentException(message);
        }
        int version = 0;
        for (int i = 0, num; i < 3; i++) {

            if (i < array.length) {
                num = Integer.parseInt(array[i]);
                if (num > 99 || num < 0) {
                    String message = String.format("%s error.", completeVersion);
                    throw new IllegalArgumentException(message);
                }
                version = version * 100 + num;
            } else {
                version *= 100;
            }

        }
        return new ServerVersion(completeVersion, version);
    }


    private static List<ServerVersion> createServerVersionList() {
        try {
            List<ServerVersion> versionList = new ArrayList<>(15);
            Field[] fields = ServerVersion.class.getDeclaredFields();
            int modifiers;
            for (Field field : fields) {
                modifiers = field.getModifiers();
                if (Modifier.isStatic(modifiers)
                        && Modifier.isPublic(modifiers)
                        && Modifier.isFinal(modifiers)
                        && field.getType() == ServerVersion.class) {
                    versionList.add((ServerVersion) field.get(null));
                }
            }
            List<ServerVersion> tempList = new ArrayList<>(versionList.size());
            tempList.addAll(versionList);
            return Collections.unmodifiableList(tempList);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }


}
