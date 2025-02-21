package xo.utility;

public class AESDecryptionException extends BaseException {

    private static final long serialVersionUID = 5541272120247285265L;

    public AESDecryptionException() {
        super("The string could not be decrypted.");
    }

    public AESDecryptionException(String mess) {
        super(mess);
    }

    public AESDecryptionException(Exception e) {
        super(e.getMessage());
    }
}