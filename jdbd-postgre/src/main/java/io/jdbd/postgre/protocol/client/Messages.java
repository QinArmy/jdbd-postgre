package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Encoding;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.qinarmy.util.HexUtils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

abstract class Messages {


    static final byte STRING_TERMINATOR = '\0';

    static final int LENGTH_SIZE = 4;

    /**
     * <ul>
     *     <li>CommandComplete</li>
     * </ul>
     */
    static final byte C = 'C';

    /**
     * <ul>
     *     <li>DataRow</li>
     * </ul>
     */
    static final byte D = 'D';

    /**
     * <ul>
     *     <li>ErrorResponse</li>
     * </ul>
     */
    static final byte E = 'E';

    static final byte R = 'R';

    /**
     * <ul>
     *     <li>NoticeResponse</li>
     * </ul>
     */
    static final byte N = 'N';

    static final byte S = 'S';

    static final byte v = 'v';

    /**
     * <ul>
     *     <li>ReadyForQuery</li>
     * </ul>
     */
    static final byte Z = 'Z';

    /**
     * <ul>
     *     <li>RowDescription</li>
     * </ul>
     */
    static final byte T = 'T';

    static final byte I = 'I';

    static final byte K = 'K';

    static final byte Q = 'Q';


    /** Specifies that the authentication was successful. See AuthenticationOk message format. */
    static final byte AUTH_OK = 0;

    /** Specifies that Kerberos V5 authentication is required. See AuthenticationKerberosV5 message format. */
    static final byte AUTH_KRB5 = 2;

    /** Specifies that a clear-text password is required. See AuthenticationCleartextPassword message format. */
    static final byte AUTH_CLEAR_TEXT = 3;

    /** Specifies that an MD5-encrypted password is required. See AuthenticationMD5Password message format. */
    static final byte AUTH_MD5 = 5;

    /** Specifies that an SCM credentials message is required. See AuthenticationSCMCredential message format. */
    static final byte AUTH_SCM = 6;

    /** Specifies that GSSAPI authentication is required. See AuthenticationGSS message format. */
    static final byte AUTH_GSS = 7;

    /** Specifies that this message contains GSSAPI or SSPI data. See AuthenticationGSSContinue message format. */
    static final byte AUTH_GSS_CONTINUE = 8;

    /** Specifies that SSPI authentication is required. See AuthenticationSSPI message format. */
    static final byte AUTH_SSPI = 9;

    /** Specifies that SASL authentication is required. See AuthenticationSASL message format. */
    static final byte AUTH_SASL = 10;

    /** Specifies that this message contains a SASL challenge. See AuthenticationSASLContinue message format. */
    static final byte AUTH_SASL_CONTINUE = 11;

    /** Specifies that SASL authentication has completed. See AuthenticationSASLFinal message format. */
    static final byte AUTH_SASL_FINAL = 12;


    static void writeString(ByteBuf message, String string) {
        message.writeBytes(string.getBytes(Encoding.CLIENT_CHARSET));
        message.writeByte(STRING_TERMINATOR);
    }


    static String readString(final ByteBuf message, Charset charset) {
        return new String(readBytesTerm(message), charset);
    }

    static byte[] readBytesTerm(final ByteBuf message) {
        final int len;
        len = message.bytesBefore(STRING_TERMINATOR);
        if (len < 0) {
            throw new IllegalArgumentException("Not found terminator of string.");
        }
        final byte[] bytes = new byte[len];
        message.readBytes(bytes);

        if (message.readByte() != STRING_TERMINATOR) {
            throw new IllegalArgumentException("Not found terminator of string.");
        }
        return bytes;
    }

    /**
     * <p>
     * Read CommandComplete message body(not include message type byte)
     * </p>
     *
     * @return command tag
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CommandComplete</a>
     */
    static String readCommandComplete(ByteBuf message, Charset charset) {
        byte[] bytes = new byte[message.readInt() - LENGTH_SIZE];
        message.readBytes(bytes);
        return new String(bytes, charset);
    }


    static boolean hasOneMessage(ByteBuf cumulateBuffer) {
        final int readableBytes = cumulateBuffer.readableBytes();
        return readableBytes > 5
                && readableBytes >= (1 + cumulateBuffer.getInt(cumulateBuffer.readerIndex() + 1));
    }

    static void skipOneMessage(ByteBuf message) {
        message.readByte();
        message.skipBytes(message.getInt(message.readerIndex()));
    }

    /**
     * @return PasswordMessage
     * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.3">AuthenticationMD5Password</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">PasswordMessage</a>
     */
    static ByteBuf md5Password(String user, String password, final byte[] salt, ByteBufAllocator allocator) {

        try {
            final MessageDigest digest;
            digest = MessageDigest.getInstance("MD5");
            byte[] tempBytes;
            // [1]. get md5( concat(password, username)  )
            digest.update(password.getBytes(StandardCharsets.UTF_8));
            digest.update(user.getBytes(StandardCharsets.UTF_8));
            tempBytes = digest.digest();
            tempBytes = HexUtils.hexEscapes(false, tempBytes, tempBytes.length);

            // [2]. get  md5([1], random-salt))
            digest.update(tempBytes);
            digest.update(salt);
            tempBytes = digest.digest();
            tempBytes = HexUtils.hexEscapes(false, tempBytes, tempBytes.length);

            // [3]. get concat('md5', [2]) and create PasswordMessage
            final ByteBuf message = allocator.buffer(8 + tempBytes.length + 1);

            message.writeByte('p'); // Byte1('p')
            message.writeZero(4); // length placeholder.

            message.writeByte('m');
            message.writeByte('d');
            message.writeByte('5');
            message.writeBytes(tempBytes);

            message.writeByte(STRING_TERMINATOR);

            writeLength(message);
            return message;
        } catch (NoSuchAlgorithmException e) {
            // never here.
            throw new RuntimeException(e);
        }

    }


    static void writeLength(ByteBuf message) {
        final int length = message.readableBytes() - 1, writerIndex = message.writerIndex();
        message.writerIndex(message.readerIndex() + 1);
        message.writeInt(length);
        message.writerIndex(writerIndex);
    }


}
