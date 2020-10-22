package io.jdbd.mysql.protocol;

public final class ServerVersion implements Comparable<ServerVersion> {

    private static final ServerVersion MIN_VERSION = new ServerVersion("0.0.0", 0, 0, 0);

    private final String completeVersion;
    private final int major;
    private final int minor;
    private final int subMinor;

    private ServerVersion(String completeVersion, int major, int minor, int subMinor) {
        this.completeVersion = completeVersion;
        this.major = major;
        this.minor = minor;
        this.subMinor = subMinor;
    }

    public int compareTo(ServerVersion other) {
        return doCompareTo(other.major, other.minor, other.subMinor);
    }

    @Override
    public String toString() {
        return this.completeVersion;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ServerVersion)) {
            return false;
        }
        ServerVersion another = (ServerVersion) obj;
        return this.major == another.major
                && this.minor == another.minor
                && this.subMinor == another.subMinor;
    }

    /**
     * Does this version meet the minimum specified by `min'?
     *
     * @param min The minimum version to compare against.
     * @return true if version meets the minimum specified by `min'
     */
    public boolean meetsMinimum(ServerVersion min) {
        return doCompareTo(min.major, min.minor, min.subMinor) >= 0;
    }

    public boolean meetsMinimum(int major, int minor, int subMinor) {
        return doCompareTo(major, minor, subMinor) >= 0;
    }


    public String getCompleteVersion() {
        return this.completeVersion;
    }

    public int getMajor() {
        return this.major;
    }

    public int getMinor() {
        return this.minor;
    }

    public int getSubMinor() {
        return this.subMinor;
    }

    private int doCompareTo(int major, int minor, int subMinor) {
        int c;
        if ((c = Integer.compare(this.major, major)) != 0) {
            return c;
        } else if ((c = Integer.compare(this.minor, minor)) != 0) {
            return c;
        }
        return Integer.compare(this.subMinor, subMinor);
    }


    /**
     * Parse the server version into major/minor/subminor.
     *
     * @param versionString string version representation
     * @return {@link ServerVersion}
     */
    public static ServerVersion parseVersion(final String versionString) {
        final int index1 = versionString.indexOf('.');
        if (index1 < 0) {
            throw new IllegalArgumentException("versionString error");
        }
        int major = Integer.parseInt(versionString.substring(0, index1));
        final int index2 = versionString.indexOf('.', index1 + 1);
        if (index2 < 0) {
            throw new IllegalArgumentException("versionString error");
        }
        int minor = Integer.parseInt(versionString.substring(index1 + 1, index2));

        final int len = versionString.length();
        for (int i = index2 + 1; i < len; i++) {
            if ((versionString.charAt(i) < '0') || (versionString.charAt(i) > '9')) {
                continue;
            }
            int subMinor = Integer.parseInt(versionString.substring(i));
            return new ServerVersion(versionString, major, minor, subMinor);
        }
        throw new IllegalArgumentException("versionString error");
    }

    public static ServerVersion getMinVersion() {
        return MIN_VERSION;
    }

    public static ServerVersion getInstance(int major, int minor, int subMinor) {
        return new ServerVersion(major + "." + minor + "." + subMinor, major, minor, subMinor);
    }

}
