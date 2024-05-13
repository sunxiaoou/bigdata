package xo.netty.backup;

public class RuleException extends RuntimeException {
    private int code;

    public RuleException(String message, int code) {
        super(message);
        this.code = code;
    }

    public RuleException(String message, Throwable cause, int code) {
        super(message, cause);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
