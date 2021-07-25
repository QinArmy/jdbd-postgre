package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

/**
 * @see PostgreConnectionTask
 * @see GssUnitTask
 */
abstract class ConnectionTaskUtils {

    private ConnectionTaskUtils() {
        throw new UnsupportedOperationException();
    }

    static final Oid SPNEGO_MECHANISM;

    static final Oid KRB5_MECHANISM;

    static {
        try {
            SPNEGO_MECHANISM = new Oid("1.3.6.1.5.5.2");
            KRB5_MECHANISM = new Oid("1.2.840.113554.1.2.2");
        } catch (GSSException e) {
            throw new RuntimeException(e);
        }
    }


    static boolean supportSpnego(GSSManager manager) throws GSSException {
        boolean support = false;
        for (Oid mech : manager.getMechs()) {
            if (mech.equals(SPNEGO_MECHANISM)) {
                support = true;
                break;
            }
        }
        return support;
    }


    static ByteBuf createSslRequestPacket(ByteBufAllocator allocator) {
        return doCreateSSlRequestPacket(allocator, false);
    }



    /*################################## blow private method ##################################*/

    /**
     * @see <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Protocol::SSLRequest</a>
     */
    private static ByteBuf doCreateSSlRequestPacket(ByteBufAllocator allocator, boolean gss) {
        final ByteBuf packet = allocator.buffer(8);
        // For historical reasons, the very first message sent by the client (the startup message) has no initial message-type byte.
        packet.writeInt(8);
        packet.writeShort(1234);
        packet.writeShort(gss ? 5680 : 5679);
        return packet;
    }


}
