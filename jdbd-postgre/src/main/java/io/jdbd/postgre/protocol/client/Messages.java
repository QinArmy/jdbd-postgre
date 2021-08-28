package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.qinarmy.util.HexUtils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;

abstract class Messages {


    static final byte STRING_TERMINATOR = '\0';

    static final byte LENGTH_SIZE = 4;

    /**
     * <ul>
     *     <li>CommandComplete</li>
     * </ul>
     */
    static final byte C = 'C';

    /**
     * <ul>
     *     <li>CopyDone</li>
     * </ul>
     */
    static final byte c = 'c';

    /**
     * <ul>
     *     <li>CopyData</li>
     * </ul>
     */
    static final byte d = 'd';

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

    /**
     * <ul>
     *     <li>CopyFail</li>
     * </ul>
     */
    static final byte f = 'f';

    /**
     * <ul>
     *     <li>CopyInResponse</li>
     * </ul>
     */
    static final byte G = 'G';

    /**
     * <ul>
     *     <li>CopyOutResponse</li>
     * </ul>
     */
    static final byte H = 'H';

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

    /**
     * <ul>
     *     <li>CopyBothResponse</li>
     * </ul>
     */
    static final byte W = 'W';


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

    static boolean hasReadyForQuery(final ByteBuf cumulateBuffer) {
        final int originalIndex = cumulateBuffer.readerIndex();
        boolean has = false;
        while (hasOneMessage(cumulateBuffer)) {
            final int msgIndex = cumulateBuffer.readerIndex(), msgType = cumulateBuffer.readByte();
            final int nextMsgIndex = msgIndex + 1 + cumulateBuffer.readInt();
            if (msgType == Z) {
                has = true;
                break;
            }
            cumulateBuffer.readerIndex(nextMsgIndex);
        }
        cumulateBuffer.readerIndex(originalIndex);
        return has;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ParameterStatus</a>
     */
    static Map<String, String> readParameterStatus(ByteBuf cumulateBuffer, Charset charset) {
        final int msgIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.readByte() != S) {
            throw new IllegalArgumentException("Non ParameterStatus");
        }
        final int nextMsgIndex = msgIndex + 1 + cumulateBuffer.readInt();

        final Map<String, String> map;
        map = Collections.singletonMap(
                Messages.readString(cumulateBuffer, charset)
                , Messages.readString(cumulateBuffer, charset)
        );

        cumulateBuffer.readerIndex(nextMsgIndex);// avoid tail filler
        return map;
    }


    static ResultSetStatus getResultSetStatus(ByteBuf cumulateBuffer) {
        final int originalIndex = cumulateBuffer.readerIndex();
        final int currentMsgType = cumulateBuffer.getByte(originalIndex);
        if (!hasOneMessage(cumulateBuffer) || (currentMsgType != C && currentMsgType != I)) {
            String m = String.format("Current message[%s] Non-CommandComplete."
                    , (char) cumulateBuffer.getByte(originalIndex));
            throw new IllegalArgumentException(m);
        }
        cumulateBuffer.readByte();
        cumulateBuffer.readerIndex(originalIndex + 1 + cumulateBuffer.readInt());

        ResultSetStatus status = ResultSetStatus.MORE_CUMULATE;
        loop:
        while (hasOneMessage(cumulateBuffer)) {
            final int msgStartIndex = cumulateBuffer.readerIndex();
            final int msgType = cumulateBuffer.readByte();
            final int nextMsgIndex = msgStartIndex + 1 + cumulateBuffer.readInt();
            switch (msgType) {
                case T:// RowDescription message
                case I:// EmptyQueryResponse message
                case C:// CommandComplete message
                    status = ResultSetStatus.MORE_RESULT;
                    break loop;
                case E:// ErrorResponse message
                case Z: // ReadyForQuery message
                    status = ResultSetStatus.NO_MORE_RESULT;
                    break loop;
                default: {
                    // here maybe NoticeResponse message
                    cumulateBuffer.readerIndex(nextMsgIndex);
                }
            }
        }
        cumulateBuffer.readerIndex(originalIndex);
        return status;
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
