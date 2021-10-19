package io.jdbd.session;

public interface Xid {

    String getGtrid();

    String getBqual();

    int getFormatId();

}
