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

    final boolean sendFractionalSeconds;

    final boolean sendFractionalSecondsForTime;

    final Environment env;

    FixedEnv(Environment env) {
        this.transformedBitIsBoolean = env.getOrDefault(MySQLKey.TRANS_FORMED_BIT_IS_BOOLEAN);
        this.functionsNeverReturnBlobs = env.getOrDefault(MySQLKey.FUNCTIONS_NEVER_RETURN_BLOBS);
        this.blobsAreStrings = env.getOrDefault(MySQLKey.BLOBS_ARE_STRINGS);
        this.maxAllowedPayload = parseMaxAllowedPacket(env);

        this.bigColumnBoundaryBytes = env.getOrMin(MySQLKey.BIG_COLUMN_BOUNDARY_BYTES, 1 << 27);
        this.sendFractionalSeconds = env.getOrDefault(MySQLKey.SEND_FRACTIONAL_SECONDS);
        this.sendFractionalSecondsForTime = env.getOrDefault(MySQLKey.SEND_FRACTIONAL_SECONDS_FOR_TIME);

        this.env = env;
    }


    /**
     * @see MySQLKey#MAX_ALLOWED_PACKET
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    private static int parseMaxAllowedPacket(final Environment env) {
        final int maxAllowedPacket;
        maxAllowedPacket = env.getOrDefault(MySQLKey.MAX_ALLOWED_PACKET);

        final int defaultValue = 1 << 26, minValue = 1024, maxValue = 1 << 30;

        final int value;
        if (maxAllowedPacket < 0 || maxAllowedPacket == defaultValue) {
            // (1 << 26) is default value , 64MB
            value = defaultValue;
        } else if (maxAllowedPacket <= minValue) {
            value = minValue;
        } else if (maxAllowedPacket >= maxValue) {
            value = maxValue; //1GB
        } else {
            value = maxAllowedPacket & (~1023); // The value should be a multiple of 1024 .
        }
        return value;
    }


}
