package io.jdbd.postgre.config;

import io.jdbd.vendor.conf.NonNameEnum;

public abstract class Enums {

    protected Enums() {
        throw new UnsupportedOperationException();
    }


    public enum SslMode implements NonNameEnum {
        disable, allow, prefer, require, verify_ca, verify_full;

        //for reflection
        public static SslMode fromText(String text) {
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

    }

    public enum AutoSave {
        always,
        never,
        conservative
    }


}
