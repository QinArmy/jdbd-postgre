package io.jdbd.mysql.protocol.conf;

public abstract class PropertyDefinitions {

    protected PropertyDefinitions() {
        throw new UnsupportedOperationException();
    }

    public enum SslMode {
        PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY, DISABLED;
    }
}
