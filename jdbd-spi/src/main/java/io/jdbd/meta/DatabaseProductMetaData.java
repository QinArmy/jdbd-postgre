package io.jdbd.meta;

public final class DatabaseProductMetaData {

    private final String productName;

    private final int majorVersion;

    private final int minorVersion;

    public DatabaseProductMetaData(String productName, int majorVersion, int minorVersion) {
        this.productName = productName;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    public String getProductName() {
        return this.productName;
    }

    public int getMajorVersion() {
        return this.majorVersion;
    }

    public int getMinorVersion() {
        return this.minorVersion;
    }
}
