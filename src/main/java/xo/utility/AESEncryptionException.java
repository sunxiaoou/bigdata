package xo.utility;

public class AESEncryptionException extends BaseException {

    private static final long serialVersionUID = -623048968173604530L;

    public AESEncryptionException() {
        super("The string could not be encrypted.");
    }
}