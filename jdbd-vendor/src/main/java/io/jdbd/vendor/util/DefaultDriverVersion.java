package io.jdbd.vendor.util;

import io.jdbd.Driver;
import io.jdbd.DriverVersion;
import io.jdbd.session.Option;
import reactor.util.annotation.Nullable;

import java.util.Objects;

public final class DefaultDriverVersion implements DriverVersion {


    public static DefaultDriverVersion from(final String name, final Class<? extends Driver> driverClass) {
        final String version = driverClass.getPackage().getImplementationVersion();
        final DefaultDriverVersion driverVersion;
        if (version == null) {
            // here, driverClass run in  module test environment.
            driverVersion = new DefaultDriverVersion(name, "0.0.0", 0, 0);
        } else {
            final int major, minor;
            major = getMajorVersion(version);
            minor = getMinorVersion(version);
            driverVersion = new DefaultDriverVersion(name, version, major, minor);
        }
        return driverVersion;
    }

    private final String name;

    private final String version;

    private final int major;

    private final int minor;

    private DefaultDriverVersion(String name, String version, int major, int minor) {
        this.name = name;
        this.version = version;
        this.major = major;
        this.minor = minor;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public int getMajor() {
        return this.major;
    }

    @Override
    public int getMinor() {
        return this.minor;
    }

    @Override
    public String getVersion() {
        return this.version;
    }

    @Override
    public int getSubMinor() {
        //TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean meetsMinimum(int major, int minor, int subMinor) {
        //TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T valueOf(Option<T> option) {
        // always null
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), this.version, this.major, this.minor);
    }

    @Override
    public boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof DriverVersion) {
            final DriverVersion o = (DriverVersion) obj;
            match = getName().equals(o.getName())
                    && this.version.equals(o.getVersion())
                    && this.major == o.getMajor()
                    && this.minor == o.getMinor();
        } else {
            match = false;
        }
        return match;
    }


    private static int getMajorVersion(final String version) {
        try {
            return Integer.parseInt(version.substring(0, getMajorVersionIndex(version)));
        } catch (NumberFormatException e) {
            throw createPacketWithErrorWay();
        }
    }

    private static int getMinorVersion(final String version) {
        final int majorIndex = getMajorVersionIndex(version);
        final int minorIndex = version.indexOf('-', majorIndex);
        if (minorIndex < 0) {
            throw createPacketWithErrorWay();
        }
        try {
            return Integer.parseInt(version.substring(majorIndex + 1, minorIndex));
        } catch (NumberFormatException e) {
            throw createPacketWithErrorWay();
        }
    }

    /**
     * @see #getMinorVersion(String)
     */
    private static int getMajorVersionIndex(@Nullable String version) {

        if (version == null) {
            // here run in  project test environment.
            throw createPacketWithErrorWay();
        }
        final int index = version.indexOf('.');
        if (index < 0) {
            throw createPacketWithErrorWay();
        }
        return index;
    }


    private static IllegalStateException createPacketWithErrorWay() {
        return new IllegalStateException("jdbd-postgre packet with error way.");
    }


}
