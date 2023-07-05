package io.jdbd;

public interface VersionSpec {

    /**
     * Retrieves the driver's major version number. Initially this should be 1.
     *
     * @return this driver's major version number
     */
    int getMajor();

    /**
     * Gets the driver's minor version number. Initially this should be 0.
     *
     * @return this driver's minor version number
     */
    int getMinor();


    String getVersion();

}
