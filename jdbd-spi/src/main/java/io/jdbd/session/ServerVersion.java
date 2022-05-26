package io.jdbd.session;

public interface ServerVersion {

    String getVersion();

    int getMajor();

    int getMinor();

    int getSubMinor();

    boolean meetsMinimum(int major, int minor, int subMinor);

}
