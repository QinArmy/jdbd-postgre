package io.jdbd.vendor.task;

public enum SslMode {

    /** Start with encrypted connection, fallback to non-encrypted (default). */
    PREFERRED,
    /** Ensure connection is encrypted. */
    REQUIRED,
    /** Ensure connection is encrypted, and client trusts server certificate. */
    VERIFY_CA,
    /**
     * Ensure connection is encrypted,and verify server certificate
     * ,and that the server certificate matches the host to which the connection is attempted
     */
    VERIFY_IDENTITY,
    /** Do not use encrypted connections. */
    DISABLED

}
