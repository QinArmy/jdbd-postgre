package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.vendor.env.HostInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * see  {@code com.mysql.cj.protocol.a.authentication.MysqlOldPasswordPlugin}
 * </p>
 */
public class MySQLOldPasswordPlugin implements AuthenticationPlugin {

    public static MySQLOldPasswordPlugin getInstance(AuthenticateAssistant protocolAssistant) {
        return new MySQLOldPasswordPlugin(protocolAssistant);
    }

    public static final String PLUGIN_NAME = "mysql_old_password";

    public static final String PLUGIN_CLASS = "com.mysql.cj.protocol.a.authentication.MysqlOldPasswordPlugin";

    private final AuthenticateAssistant protocolAssistant;

    private final HostInfo hostInfo;

    private MySQLOldPasswordPlugin(AuthenticateAssistant protocolAssistant) {
        this.protocolAssistant = protocolAssistant;
        this.hostInfo = protocolAssistant.getHostInfo();
    }


    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean requiresConfidentiality() {
        return false;
    }

    @Override
    public List<ByteBuf> nextAuthenticationStep(ByteBuf fromServer) {
        String password = hostInfo.getPassword();
        ByteBuf payloadBuf;
        if (MySQLStrings.isEmpty(password)) {
            payloadBuf = Unpooled.EMPTY_BUFFER;
        } else {
            String seed = Packets.readStringTerm(fromServer, Charset.defaultCharset()).substring(0, 8);
            String cryptString = newCrypt(password, seed, this.protocolAssistant.getPasswordCharset());
            byte[] payloadBytes = cryptString.getBytes();

            payloadBuf = this.protocolAssistant.allocator().buffer(payloadBytes.length);
            payloadBuf.writeBytes(payloadBytes);
        }
        return Collections.singletonList(payloadBuf);
    }

    // Right from Monty's code
    private static String newCrypt(String password, String seed, Charset encoding) {
        byte b;
        double d;

        long[] pw = newHash(seed.getBytes());
        long[] msg = hashPre41Password(password, encoding);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];

        for (int i = 0; i < seed.length(); i++) {
            seed1 = ((seed1 * 3) + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) java.lang.Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }

        seed1 = ((seed1 * 3) + seed2) % max;
        seed2 = (seed1 + seed2 + 33) % max;
        d = (double) seed1 / (double) max;
        b = (byte) java.lang.Math.floor(d * 31);

        for (int i = 0; i < seed.length(); i++) {
            chars[i] ^= (char) b;
        }

        return new String(chars);
    }

    private static long[] hashPre41Password(String password, Charset encoding) {
        // remove white spaces and convert to bytes
        return newHash(password.replaceAll("\\s", "").getBytes(encoding));
    }

    private static long[] newHash(byte[] password) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;

        for (byte b : password) {
            tmp = 0xff & b;
            nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
            nr2 += ((nr2 << 8) ^ nr);
            add += tmp;
        }

        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;

        return result;
    }
}
