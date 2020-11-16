package io.jdbd.mysql.protocol.client;

import java.nio.charset.Charset;

interface DecoderAdjutant {

    int obtainNegotiatedCapability();

    Charset obtainCharsetResults();

}
