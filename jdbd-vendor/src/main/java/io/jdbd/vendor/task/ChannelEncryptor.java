package io.jdbd.vendor.task;


public interface ChannelEncryptor {

    void addSsl(SslWrapper sslWrapper);

    void addGssContext(GssWrapper gssWrapper);


}
