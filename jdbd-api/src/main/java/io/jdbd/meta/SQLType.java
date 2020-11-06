package io.jdbd.meta;

public interface SQLType {

    /**
     * @return upper case name.
     */
    String name();

    int code();

    boolean useInProtocol();

}
