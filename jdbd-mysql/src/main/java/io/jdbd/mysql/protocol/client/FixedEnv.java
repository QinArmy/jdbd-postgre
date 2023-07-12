package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.env.Environment;
import io.jdbd.mysql.env.MySQLKey;

/**
 * <p>
 * This class is base class of {@link ClientProtocolFactory}.
 * </p>
 *
 * @since 1.0
 */
abstract class FixedEnv {

    final boolean transformedBitIsBoolean;

    final boolean functionsNeverReturnBlobs;

    final boolean blobsAreStrings;

    final int bigColumnBoundaryBytes;

    final int maxAllowedPayload;

    final Environment env;

    FixedEnv(Environment env) {
        this.transformedBitIsBoolean = env.getOrDefault(MySQLKey.TRANS_FORMED_BIT_IS_BOOLEAN);
        this.functionsNeverReturnBlobs = env.getOrDefault(MySQLKey.FUNCTIONS_NEVER_RETURN_BLOBS);
        this.blobsAreStrings = env.getOrDefault(MySQLKey.BLOBS_ARE_STRINGS);
        this.maxAllowedPayload = 1 << 30; //TODO

        this.bigColumnBoundaryBytes = env.getOrMin(MySQLKey.BIG_COLUMN_BOUNDARY_BYTES, 1 << 27);
        this.env = env;
    }

}
