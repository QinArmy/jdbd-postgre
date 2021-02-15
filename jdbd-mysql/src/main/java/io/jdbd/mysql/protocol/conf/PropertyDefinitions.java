package io.jdbd.mysql.protocol.conf;

public abstract class PropertyDefinitions {

    protected PropertyDefinitions() {
        throw new UnsupportedOperationException();
    }

    public enum SslMode {
        PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY, DISABLED;
    }

    public enum ZeroDatetimeBehavior { // zeroDateTimeBehavior
        CONVERT_TO_NULL, EXCEPTION, ROUND;
    }
}
