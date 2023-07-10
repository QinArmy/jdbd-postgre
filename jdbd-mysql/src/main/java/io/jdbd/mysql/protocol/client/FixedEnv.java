package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.env.MySQLEnvironment;
import io.jdbd.mysql.env.MySQLKey;

abstract class FixedEnv {

    final boolean transformedBitIsBoolean;

    final boolean functionsNeverReturnBlobs;

    final boolean blobsAreStrings;

    FixedEnv(MySQLEnvironment env) {
        this.transformedBitIsBoolean = env.getOrDefault(MySQLKey.TRANS_FORMED_BIT_IS_BOOLEAN);
        this.functionsNeverReturnBlobs = env.getOrDefault(MySQLKey.FUNCTIONS_NEVER_RETURN_BLOBS);
        this.blobsAreStrings = env.getOrDefault(MySQLKey.BLOBS_ARE_STRINGS);
    }

}
