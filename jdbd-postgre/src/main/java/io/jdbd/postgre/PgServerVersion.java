package io.jdbd.postgre;

import io.jdbd.ServerVersion;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class PgServerVersion implements Comparable<PgServerVersion>, ServerVersion {


    public static final PgServerVersion INVALID = PgServerVersion.parse("0.0.0");

    public static final PgServerVersion V8_1 = PgServerVersion.parse("8.1.0");
    public static final PgServerVersion V8_2 = PgServerVersion.parse("8.2.0");
    public static final PgServerVersion V8_3 = PgServerVersion.parse("8.3.0");
    public static final PgServerVersion V8_4 = PgServerVersion.parse("8.4.0");

    public static final PgServerVersion V9_0 = PgServerVersion.parse("9.0.0");
    public static final PgServerVersion V9_1 = PgServerVersion.parse("9.1.0");
    public static final PgServerVersion V9_2 = PgServerVersion.parse("9.2.0");
    public static final PgServerVersion V9_3 = PgServerVersion.parse("9.3.0");

    public static final PgServerVersion V9_4 = PgServerVersion.parse("9.4.0");
    public static final PgServerVersion V9_5 = PgServerVersion.parse("9.5.0");
    public static final PgServerVersion V9_6 = PgServerVersion.parse("9.6.0");
    public static final PgServerVersion V10 = PgServerVersion.parse("10");

    public static final PgServerVersion V11 = PgServerVersion.parse("11");
    public static final PgServerVersion V12 = PgServerVersion.parse("12");


    static final List<PgServerVersion> CACHE_VERSION_LIST;

    static {
        CACHE_VERSION_LIST = createServerVersionList();
    }


    private final String version;

    private final int versionNumber;

    private PgServerVersion(String version, int versionNumber) {
        this.version = version;
        this.versionNumber = versionNumber;
    }


    @Override
    public final String getVersion() {
        return this.version;
    }

    public final int getVersionNumber() {
        return this.versionNumber;
    }

    @Override
    public final int getMajor() {
        return this.versionNumber / 10000;
    }

    @Override
    public final int getMinor() {
        return (this.versionNumber % 10000) / 100;
    }

    @Override
    public final int getSubMinor() {
        return this.versionNumber % 100;
    }


    @Override
    public final int compareTo(PgServerVersion o) {
        return this.versionNumber - o.versionNumber;
    }

    @Override
    public final boolean equals(@Nullable Object nullable) {
        final boolean match;
        if (nullable == this) {
            match = true;
        } else if (nullable instanceof PgServerVersion) {
            PgServerVersion v = (PgServerVersion) nullable;
            return v.versionNumber == this.versionNumber;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final int hashCode() {
        return this.versionNumber;
    }

    @Override
    public String toString() {
        return new StringBuilder("ServerVersion{")
                .append("completeVersion=")
                .append(this.version)
                .append(",version=")
                .append(this.versionNumber)
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
    public static PgServerVersion from(final String completeVersion) {
        for (PgServerVersion serverVersion : CACHE_VERSION_LIST) {
            if (serverVersion.version.equals(completeVersion)) {
                return serverVersion;
            }
        }
        return parse(completeVersion);
    }

    private static PgServerVersion parse(final String completeVersion) {
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
        return new PgServerVersion(completeVersion, version);
    }


    private static List<PgServerVersion> createServerVersionList() {
        try {
            List<PgServerVersion> versionList = new ArrayList<>(15);
            Field[] fields = PgServerVersion.class.getDeclaredFields();
            int modifiers;
            for (Field field : fields) {
                modifiers = field.getModifiers();
                if (Modifier.isStatic(modifiers)
                        && Modifier.isPublic(modifiers)
                        && Modifier.isFinal(modifiers)
                        && field.getType() == PgServerVersion.class) {
                    versionList.add((PgServerVersion) field.get(null));
                }
            }
            List<PgServerVersion> tempList = new ArrayList<>(versionList.size());
            tempList.addAll(versionList);
            return Collections.unmodifiableList(tempList);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }


}
