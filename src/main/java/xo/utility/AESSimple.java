package xo.utility;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Random;

/**
 * An easy-to-use AES encryption class.
 */
public class AESSimple {
    private static String encryptionKey;

    private static byte[] parseBase64Binary(String chipher) {
        return org.apache.commons.codec.binary.Base64.decodeBase64(chipher);
    }

    private static String printBase64Binary(byte[] chiper) {
        return org.apache.commons.codec.binary.Base64.encodeBase64String(chiper);
    }

    private static String generateHexRandomString(int length) {
        char[] hexChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(hexChars[random.nextInt(hexChars.length)]);
        }
        return sb.toString();
    }

    /**
     * Encrypt a string (AES-128/ECB/NoPadding)
     *
     * @param key  Key
     * @param text Plain text.
     * @return Cipher in base64 format.
     * @throws AESEncryptionException
     */
    public static String encrypt(String key, String text) {
        try {
            // Random salt (8 bytes)
            byte[] salt = new byte[RANDOM_SALT_LEN];
            new SecureRandom().nextBytes(salt);

            final Charset utf8 = Charset.forName("UTF-8");
            byte[] keyUTF8 = key.getBytes(utf8);
            byte[] textUTF8 = text.getBytes(utf8);

            // AES-Key=MD5(Key + 8-Byte-Salt)
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(keyUTF8);
            md5.update(salt);
            byte[] keyaes = md5.digest();

            // 16-Byte-MD5=MD5(Text + Pending-Zeros)
            int lenbuf = RANDOM_SALT_LEN
                    + ((SHA_128_DIGEST_LEN + textUTF8.length + SHA_128_DIGEST_LEN - 1) / SHA_128_DIGEST_LEN)
                    * SHA_128_DIGEST_LEN;

            // If text is less than 32 characters,
            // then use 32, this is to make it more secure.
            if (lenbuf < RANDOM_SALT_LEN + SHA_128_DIGEST_LEN + SHA_128_DIGEST_LEN * 2) {
                lenbuf = RANDOM_SALT_LEN + SHA_128_DIGEST_LEN + SHA_128_DIGEST_LEN * 2;
            }

            byte[] cipher = new byte[lenbuf];

            for (int i = 0; i < textUTF8.length; ++i) {
                cipher[RANDOM_SALT_LEN + SHA_128_DIGEST_LEN + i] = textUTF8[i];
            }

            for (int i = RANDOM_SALT_LEN + SHA_128_DIGEST_LEN + textUTF8.length; i < cipher.length; ++i) {
                cipher[i] = 0;
            }

            md5.reset();
            md5.update(cipher, RANDOM_SALT_LEN + SHA_128_DIGEST_LEN,
                    cipher.length - RANDOM_SALT_LEN - SHA_128_DIGEST_LEN);
            md5.digest(cipher, RANDOM_SALT_LEN, SHA_128_DIGEST_LEN);

            for (int i = 0; i < RANDOM_SALT_LEN; ++i) {
                cipher[i] = salt[i];
            }

            // Encryption.
            SecretKeySpec keySpec = new SecretKeySpec(keyaes, "AES");
            Cipher aes = Cipher.getInstance("AES/ECB/NoPadding");
            aes.init(Cipher.ENCRYPT_MODE, keySpec);
            aes.doFinal(cipher, RANDOM_SALT_LEN, cipher.length - RANDOM_SALT_LEN, cipher, RANDOM_SALT_LEN);

            return printBase64Binary(cipher);
        } catch (Exception e) {
            throw new AESEncryptionException();
        }
    }

    /**
     * Decrypt a string (AES-128/ECB/NoPadding)
     *
     * @param key    Key
     * @param cipher Cipher in base64 format.
     * @return Plain text.
     * @throws AESDecryptionException
     */
    public static String decrypt(String key, String cipher) {
        try {
            byte[] data = parseBase64Binary(cipher);

            if (data.length < RANDOM_SALT_LEN + SHA_128_DIGEST_LEN
                    || (data.length - RANDOM_SALT_LEN) % SHA_128_DIGEST_LEN != 0) {
                throw new AESDecryptionException("length is wrong");
            }

            final Charset utf8 = Charset.forName("UTF-8");
            byte[] keyUTF8 = key.getBytes(utf8);

            // AES-Key=MD5(Key + 8-Byte-Salt)
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(keyUTF8);
            md5.update(data, 0, RANDOM_SALT_LEN);
            byte[] keyaes = md5.digest();

            // Decryption.
            SecretKeySpec keySpec = new SecretKeySpec(keyaes, "AES");
            Cipher aes = Cipher.getInstance("AES/ECB/NoPadding");
            aes.init(Cipher.DECRYPT_MODE, keySpec);
            aes.doFinal(data, RANDOM_SALT_LEN, data.length - RANDOM_SALT_LEN, data, RANDOM_SALT_LEN);

            md5.reset();
            md5.update(data, RANDOM_SALT_LEN + SHA_128_DIGEST_LEN, data.length - RANDOM_SALT_LEN - SHA_128_DIGEST_LEN);
            byte[] digest = md5.digest();

            for (int i = 0; i < SHA_128_DIGEST_LEN; ++i) {
                if (digest[i] != data[RANDOM_SALT_LEN + i]) {
                    throw new AESDecryptionException();
                }
            }

            int index;
            for (index = RANDOM_SALT_LEN + SHA_128_DIGEST_LEN; index < data.length; ++index) {
                if (data[index] == 0) {
                    break;
                }
            }

            return new String(data, RANDOM_SALT_LEN + SHA_128_DIGEST_LEN, index - RANDOM_SALT_LEN - SHA_128_DIGEST_LEN,
                    utf8);
        } catch (Exception e) {
            throw new AESDecryptionException();
        }
    }

    private static final int RANDOM_SALT_LEN = 8;
    private static final int SHA_128_DIGEST_LEN = 16;
    private static final int SHA_256_DIGEST_LEN = 32;


    public static String encryptSHA256(String text){
        // 默认 OFB 模式
        return encryptSHA256(encryptionKey, text, MODE_OFB);
    }

    /**
     * Encrypt a string (AES-128/ECB/NoPadding)
     *
     * @param key  Key
     * @param text Plain text.
     * @param mode encryption mode
     * @return Cipher in base64 format.
     * @throws AESEncryptionException
     */

    public static String encryptSHA256(String key, String text, String mode) {
        try {
            // Random salt (8 bytes)
            byte[] salt = new byte[RANDOM_SALT_LEN];
            new SecureRandom().nextBytes(salt);

            final Charset utf8 = Charset.forName("UTF-8");
            byte[] keyUTF8 = key.getBytes(utf8);
            byte[] textUTF8 = text.getBytes(utf8);

            // AES-Key=MD5(Key + 8-Byte-Salt)
            MessageDigest md5 = MessageDigest.getInstance("SHA-256");
            md5.update(keyUTF8);
            md5.update(salt);
            byte[] keyaes = md5.digest();

            // 32-Byte-MD5=MD5(Text + Pending-Zeros)
            int lenbuf = RANDOM_SALT_LEN
                    + ((SHA_256_DIGEST_LEN + textUTF8.length + SHA_256_DIGEST_LEN - 1) / SHA_256_DIGEST_LEN)
                    * SHA_256_DIGEST_LEN;

            // If text is less than 104 characters,
            // then use 104, this is to make it more secure.
            if (lenbuf < RANDOM_SALT_LEN + SHA_256_DIGEST_LEN + SHA_256_DIGEST_LEN * 2) {
                lenbuf = RANDOM_SALT_LEN + SHA_256_DIGEST_LEN + SHA_256_DIGEST_LEN * 2;
            }

            byte[] cipher = new byte[lenbuf];

            for (int i = 0; i < textUTF8.length; ++i) {
                cipher[RANDOM_SALT_LEN + SHA_256_DIGEST_LEN + i] = textUTF8[i];
            }

            for (int i = RANDOM_SALT_LEN + SHA_256_DIGEST_LEN + textUTF8.length; i < cipher.length; ++i) {
                cipher[i] = 0;
            }

            md5.reset();
            md5.update(cipher, RANDOM_SALT_LEN + SHA_256_DIGEST_LEN,
                    cipher.length - RANDOM_SALT_LEN - SHA_256_DIGEST_LEN);
            md5.digest(cipher, RANDOM_SALT_LEN, SHA_256_DIGEST_LEN);

            for (int i = 0; i < RANDOM_SALT_LEN; ++i) {
                cipher[i] = salt[i];
            }

            // Encryption.
            SecretKeySpec keySpec = new SecretKeySpec(keyaes, "AES");
            Cipher aes;
            if (MODE_OFB.equalsIgnoreCase(mode)) {
                byte[] ivBit128 = new byte[16];
                IvParameterSpec ivParameterSpec = new IvParameterSpec(ivBit128);
                aes = Cipher.getInstance("AES/OFB/NoPadding");
                aes.init(Cipher.ENCRYPT_MODE, keySpec, ivParameterSpec);
            } else {
                aes = Cipher.getInstance("AES/ECB/NoPadding");
                aes.init(Cipher.ENCRYPT_MODE, keySpec);
            }
            aes.doFinal(cipher, RANDOM_SALT_LEN, cipher.length - RANDOM_SALT_LEN, cipher, RANDOM_SALT_LEN);

            return printBase64Binary(cipher);
        } catch (Exception e) {
            throw new AESEncryptionException();
        }
    }

    public static final String MODE_ECB = "ECB";
    public static final String MODE_OFB = "OFB";

    /**
     * Decrypt a string (AES-128/ECB/NoPadding)
     *
     * @param key    Key
     * @param cipher Cipher in base64 format.
     * @return Plain text.
     * @throws AESDecryptionException
     */
    public static String decryptSHA256(String key, String cipher) {
        return decryptSHA256(key, cipher, MODE_ECB);
    }

    /**
     * <pre>
     * 支持模式ECB,OFB.默认模式：ECB
     * </pre>
     */
    public static String decryptSHA256(String key, String cipher, String mode) {
        try {
            byte[] data = parseBase64Binary(cipher);
//          For CC don't support NOPADDING don't check the length.
//            if (data.length < RANDOM_SALT_LEN + SHA_256_DIGEST_LEN
//                    || (data.length - RANDOM_SALT_LEN) % SHA_256_DIGEST_LEN != 0) {
//                throw new AESDecryptionException();
//            }

            final Charset utf8 = Charset.forName("UTF-8");
            byte[] keyUTF8 = key.getBytes(utf8);

            // AES-Key=MD5(Key + 8-Byte-Salt)
            MessageDigest md5 = MessageDigest.getInstance("SHA-256");
            md5.update(keyUTF8);
            md5.update(data, 0, RANDOM_SALT_LEN);
            byte[] keyaes = md5.digest();

            // Decryption.
            SecretKeySpec keySpec = new SecretKeySpec(keyaes, "AES");
            Cipher aes;
            if (MODE_OFB.equalsIgnoreCase(mode)) {
                byte[] ivBit128 = new byte[16];
                IvParameterSpec ivParameterSpec = new IvParameterSpec(ivBit128);
                aes = Cipher.getInstance("AES/OFB/NoPadding");
                aes.init(Cipher.DECRYPT_MODE, keySpec, ivParameterSpec);
            } else {
                aes = Cipher.getInstance("AES/ECB/NoPadding");
                aes.init(Cipher.DECRYPT_MODE, keySpec);
            }
            aes.doFinal(data, RANDOM_SALT_LEN, data.length - RANDOM_SALT_LEN, data, RANDOM_SALT_LEN);

            md5.reset();
            md5.update(data, RANDOM_SALT_LEN + SHA_256_DIGEST_LEN, data.length - RANDOM_SALT_LEN - SHA_256_DIGEST_LEN);
            byte[] digest = md5.digest();

//            for (int i = 0; i < SHA_256_DIGEST_LEN; ++i) {
//                if (digest[i] != data[RANDOM_SALT_LEN + i]) {
//                    throw new AESDecryptionException();
//                }
//            }

            int index;
            for (index = RANDOM_SALT_LEN + SHA_256_DIGEST_LEN; index < data.length; ++index) {
                if (data[index] == 0) {
                    break;
                }
            }

            return new String(data, RANDOM_SALT_LEN + SHA_256_DIGEST_LEN, index - RANDOM_SALT_LEN - SHA_256_DIGEST_LEN,
                    utf8);
        } catch (Exception e) {
            throw new AESDecryptionException(e);
        }
    }

    public static void main(String[] args) {
        encryptionKey = generateHexRandomString(32);
        String password = "Info1234@";
        String aes = AESSimple.encryptSHA256(encryptionKey, password, AESSimple.MODE_OFB);
        System.out.println("aes = " + aes);
        String plain = AESSimple.decryptSHA256(encryptionKey, aes, AESSimple.MODE_OFB);
        System.out.println("plain = " + plain);
    }
}