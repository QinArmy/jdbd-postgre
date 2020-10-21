package io.jdbd.mysql.protocol.authentication;

import com.mysql.cj.exceptions.AssertionFailedException;

import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

abstract class AuthenticateUtils {

    protected AuthenticateUtils() {
        throw new UnsupportedOperationException();
    }


    private static final int CACHING_SHA2_DIGEST_LENGTH = 32;

    /**
     * Encrypt/Decrypt function used for password encryption in authentication
     * <p>
     * Simple XOR is used here but it is OK as we encrypt random strings
     *
     * @param from     IN Data for encryption
     * @param to       OUT Encrypt data to the buffer (may be the same)
     * @param scramble IN Scramble used for encryption
     * @param length   IN Length of data to encrypt
     */
    public static void xorString(byte[] from, byte[] to, byte[] scramble, int length) {
        int pos = 0;
        int scrambleLength = scramble.length;

        while (pos < length) {
            to[pos] = (byte) (from[pos] ^ scramble[pos % scrambleLength]);
            pos++;
        }
    }


    /**
     * Scrambling for caching_sha2_password plugin.
     *
     * <pre>
     * Scramble = XOR(SHA2(password), SHA2(SHA2(SHA2(password)), Nonce))
     * </pre>
     *
     * @param password password
     * @param seed     seed
     * @return bytes
     * @throws DigestException if an error occurs
     */
    public static byte[] scrambleCachingSha2(byte[] password, byte[] seed) throws DigestException {
        /*
         * Server does it in 4 steps (see sql/auth/sha2_password_common.cc Generate_scramble::scramble method):
         *
         * SHA2(src) => digest_stage1
         * SHA2(digest_stage1) => digest_stage2
         * SHA2(digest_stage2, m_rnd) => scramble_stage1
         * XOR(digest_stage1, scramble_stage1) => scramble
         */
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionFailedException(ex);
        }

        byte[] dig1 = new byte[CACHING_SHA2_DIGEST_LENGTH];
        byte[] dig2 = new byte[CACHING_SHA2_DIGEST_LENGTH];
        byte[] scramble1 = new byte[CACHING_SHA2_DIGEST_LENGTH];

        // SHA2(src) => digest_stage1
        md.update(password, 0, password.length);
        md.digest(dig1, 0, CACHING_SHA2_DIGEST_LENGTH);
        md.reset();

        // SHA2(digest_stage1) => digest_stage2
        md.update(dig1, 0, dig1.length);
        md.digest(dig2, 0, CACHING_SHA2_DIGEST_LENGTH);
        md.reset();

        // SHA2(digest_stage2, m_rnd) => scramble_stage1
        md.update(dig2, 0, dig1.length);
        md.update(seed, 0, seed.length);
        md.digest(scramble1, 0, CACHING_SHA2_DIGEST_LENGTH);

        // XOR(digest_stage1, scramble_stage1) => scramble
        byte[] mysqlScrambleBuff = new byte[CACHING_SHA2_DIGEST_LENGTH];
        xorString(dig1, mysqlScrambleBuff, scramble1, CACHING_SHA2_DIGEST_LENGTH);

        return mysqlScrambleBuff;
    }


}
