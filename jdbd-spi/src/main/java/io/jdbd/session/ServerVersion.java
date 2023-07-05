package io.jdbd.session;

import io.jdbd.VersionSpec;

public interface ServerVersion extends VersionSpec {


    int getSubMinor();

    boolean meetsMinimum(int major, int minor, int subMinor);

}
