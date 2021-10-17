package io.jdbd.session;

public interface ServerVersion {

    String getVersion();

    int getMajor();

    int getMinor();

    int getSubMinor();

}
