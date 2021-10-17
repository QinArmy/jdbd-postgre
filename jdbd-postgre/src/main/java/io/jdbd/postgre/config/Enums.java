package io.jdbd.postgre.config;

import io.qinarmy.env.convert.NonNameEnum;

public abstract class Enums {

    protected Enums() {
        throw new UnsupportedOperationException();
    }


    public enum SslMode implements NonNameEnum {
        disable, allow, prefer, require, verify_ca, verify_full;

        //for reflection
        public static SslMode valueOfText(String text) {
            final SslMode sslMode;
            switch (text) {
                case "disable":
                    sslMode = disable;
                    break;
                case "allow":
                    sslMode = allow;
                    break;
                case "prefer":
                    sslMode = prefer;
                    break;
                case "require":
                    sslMode = require;
                    break;
                case "verify-ca":
                    sslMode = verify_ca;
                    break;
                case "verify-full":
                    sslMode = verify_full;
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Unknown value[%s]", text));
            }
            return sslMode;
        }

        public final boolean needSslEnc() {
            return this != disable
                    && this != allow // Allow ==> start with plaintext, use encryption if required by server
                    ;
        }

    }

    public enum AutoSave {
        always,
        never,
        conservative
    }

    public enum GSSEncMode {

        /**
         * Do not use encrypted connections.
         */
        DISABLE("disable"),

        /**
         * Start with non-encrypted connection, then try encrypted one.
         */
        ALLOW("allow"),

        /**
         * Start with encrypted connection, fallback to non-encrypted (default).
         */
        PREFER("prefer"),

        /**
         * Ensure connection is encrypted.
         */
        REQUIRE("require");


        public final String value;

        GSSEncMode(String value) {
            this.value = value;
        }

        public final boolean requireEncryption() {
            return this == REQUIRE;
        }

        public final boolean needGssEnc() {
            return this != DISABLE
                    && this != ALLOW // start with plain text and let the server request it
                    ;
        }

    }


}
