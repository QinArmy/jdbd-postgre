package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.env.PgKey;
import io.jdbd.vendor.env.Environment;

abstract class ProtocolFactory {


    final Environment env;

    final int prepareThreshold;

    ProtocolFactory(Environment env) {
        this.env = env;
        this.prepareThreshold = env.getOrDefault(PgKey.PREPARE_THRESHOLD);
    }


}
