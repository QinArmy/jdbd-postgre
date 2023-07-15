package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.env.Environment;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLStrings;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

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

    final int maxAllowedPacket;

    final boolean sendFractionalSeconds;

    final boolean sendFractionalSecondsForTime;

    final int blobSendChunkSize;

    final Map<String, Class<? extends AuthenticationPlugin>> authPlugMap;

    final Map<String, Charset> customCharsetMap;

    final Environment env;

    FixedEnv(Environment env) {
        this.transformedBitIsBoolean = env.getOrDefault(MySQLKey.TRANS_FORMED_BIT_IS_BOOLEAN);
        this.functionsNeverReturnBlobs = env.getOrDefault(MySQLKey.FUNCTIONS_NEVER_RETURN_BLOBS);
        this.blobsAreStrings = env.getOrDefault(MySQLKey.BLOBS_ARE_STRINGS);
        this.maxAllowedPacket = parseMaxAllowedPacket(env);

        this.bigColumnBoundaryBytes = env.getInRange(MySQLKey.BIG_COLUMN_BOUNDARY_BYTES, Packets.MAX_PAYLOAD, 1 << 27);
        this.sendFractionalSeconds = env.getOrDefault(MySQLKey.SEND_FRACTIONAL_SECONDS);
        this.sendFractionalSecondsForTime = env.getOrDefault(MySQLKey.SEND_FRACTIONAL_SECONDS_FOR_TIME);
        this.blobSendChunkSize = env.getInRange(MySQLKey.BLOB_SEND_CHUNK_SIZE, 1024, this.maxAllowedPacket - Packets.HEADER_SIZE);

        this.authPlugMap = PluginUtils.createPluginClassMap(env);
        this.customCharsetMap = createCustomCharsetMap(env);
        this.env = env;
    }


    /**
     * @see MySQLKey#MAX_ALLOWED_PACKET
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    private static int parseMaxAllowedPacket(final Environment env) {
        final int maxAllowedPacket;
        maxAllowedPacket = env.getOrDefault(MySQLKey.MAX_ALLOWED_PACKET);

        final int defaultValue = MySQLKey.MAX_ALLOWED_PACKET.defaultValue, minValue = 1024, maxValue = 1 << 30;

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


    /**
     * @return a unmodified map
     * @throws io.jdbd.JdbdException when {@link MySQLKey#CUSTOM_CHARSET_MAPPING} value error.
     */
    private static Map<String, Charset> createCustomCharsetMap(final Environment env) {
        final String mappingValue;
        mappingValue = env.get(MySQLKey.CUSTOM_CHARSET_MAPPING);

        if (!MySQLStrings.hasText(mappingValue)) {
            return Collections.emptyMap();
        }
        final String[] pairs = mappingValue.split(";");
        String[] valuePair;
        final Map<String, Charset> tempMap = MySQLCollections.hashMap((int) (pairs.length / 0.75F));
        for (String pair : pairs) {
            valuePair = pair.split(":");
            if (valuePair.length != 2) {
                String m = String.format("%s value format error.", MySQLKey.CUSTOM_CHARSET_MAPPING);
                throw new JdbdException(m);
            }
            tempMap.put(valuePair[0], Charset.forName(valuePair[1]));
        }
        return Collections.unmodifiableMap(tempMap);
    }


}
