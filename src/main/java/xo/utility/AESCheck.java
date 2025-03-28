package xo.utility;

import javax.crypto.Cipher;

public class AESCheck {
    public static void main(String[] args) throws Exception {
        int maxKeyLen = Cipher.getMaxAllowedKeyLength("AES");
        System.out.println("Max AES key length: " + maxKeyLen);
    }
}
