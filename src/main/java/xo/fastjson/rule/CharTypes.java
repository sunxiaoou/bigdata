package xo.fastjson.rule;
//import static com.I2information.i2soft.replicator.sql.parser.LayoutCharacters.EOI;

public class CharTypes {
    final static byte EOI    = 0x1A;
    private final static boolean[] hexFlags = new boolean[256];

    static {
        for (char c = 0; c < hexFlags.length; ++c) {
            if (c >= 'A' && c <= 'F') {
                hexFlags[c] = true;
            } else if (c >= 'a' && c <= 'f') {
                hexFlags[c] = true;
            } else if (c >= '0' && c <= '9') {
                hexFlags[c] = true;
            }
        }
    }
    public static boolean isHex(char c) {
        return c < 256 && hexFlags[c];
    }
    public static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }
    private final static boolean[] firstIdentifierFlags = new boolean[256];
    static {
        for (char c = 0; c < firstIdentifierFlags.length; ++c) {
            if (c >= 'A' && c <= 'Z') {
                firstIdentifierFlags[c] = true;
            } else if (c >= 'a' && c <= 'z') {
                firstIdentifierFlags[c] = true;
            }
        }
        firstIdentifierFlags['`'] = true;
        firstIdentifierFlags['_'] = true;
        firstIdentifierFlags['$'] = true;
    }
    public static boolean isFirstIdentifierChar(char c) {
        if (c <= firstIdentifierFlags.length) {
            return firstIdentifierFlags[c];
        }
        return c != '　' && c != '，';
    }
    private final static String[] stringCache = new String[256];
    private final static boolean[] identifierFlags = new boolean[256];
    static {
        for (char c = 0; c < identifierFlags.length; ++c) {
            if (c >= 'A' && c <= 'Z') {
                identifierFlags[c] = true;
            } else if (c >= 'a' && c <= 'z') {
                identifierFlags[c] = true;
            } else if (c >= '0' && c <= '9') {
                identifierFlags[c] = true;
            }
        }
        // identifierFlags['`'] = true;
        identifierFlags['_'] = true;
        identifierFlags['$'] = true;
        identifierFlags['#'] = true;
        for (int i = 0; i < identifierFlags.length; i++) {
            if (identifierFlags[i]) {
                char ch = (char) i;
                stringCache[i] = Character.toString(ch);
            }
        }
    }
    public static boolean isIdentifierChar(char c) {
        if (c <= identifierFlags.length) {
            return identifierFlags[c];
        }
        return c != '　' && c != '，' && c != '）' && c != '（';
    }
    public static String valueOf(char ch) {
        if (ch < stringCache.length) {
            return stringCache[ch];
        }
        return null;
    }
    private final static boolean[] whitespaceFlags = new boolean[256];
    static {
        for (int i = 0; i <= 32; ++i) {
            whitespaceFlags[i] = true;
        }
        
        whitespaceFlags[EOI] = false;
        for (int i = 0x7F; i <= 0xA0; ++i) {
            whitespaceFlags[i] = true;
        }
   
        whitespaceFlags[160] = true; // 鐗瑰埆澶勭悊
    }
    /**
     * @return false if {@link LayoutCharacters#EOI}
     */
    public static boolean isWhitespace(char c) {
        return (c <= whitespaceFlags.length && whitespaceFlags[c]) //
                || c == '　'; // Chinese space
    }
    public static String trim(String value) {
        int len = value.length();
        int st = 0;
        while ((st < len) && (isWhitespace(value.charAt(st)))) {
            st++;
        }
        while ((st < len) && isWhitespace(value.charAt(len - 1))) {
            len--;
        }
        return ((st > 0) || (len < value.length())) ? value.substring(st, len) : value;
    }
}
