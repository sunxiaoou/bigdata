package xo.netty.backup;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static String readFile(String path) {
        FileInputStream fis;
        try {
            fis = new FileInputStream(path);
        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
        byte[] buf;
        try {
            buf = new byte[fis.available()];
            fis.read(buf);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
        return new String(buf);
    }

    /**
     * convert byte to int, the byte length is between 1 and 4
     */
    public static int convertByteToInt(byte[] b) {
        if (b.length > 4) {
            LOG.error("can't convert byte to int: byte[capacity].length greater then 4");
            throw new RuntimeException("can't convert byte to int: byte[capacity].length greater then 4");
        }
        int value = 0;
        for (byte b1 : b) {
            value = (value << 8) | (b1 & 0xff);
        }
        return value;
    }

    /**
     * convert int to byte[]. used to shorten the store size
     * eg: convertIntToByte(10 << 8, 3),means use 3 bytes to represent a int type
     * @param int_ int
     * @param capacity byte[capacity], the size of capacity is between 1 and 4
     */
    public static byte[] convertIntToByte(int int_, int capacity) {
        if (capacity > 4 || capacity <= 0) {
            LOG.error("can't convert int to byte: byte[capacity].length is not between 1 and 4");
            throw new RuntimeException("can't convert int to byte: byte[capacity].length is not between 1 and 4");
        }
        int_ = int_ << 8 * (4-capacity);
        byte[] bytes = new byte[capacity];
        for (int cap = 0; cap < capacity; cap++) {
            bytes[cap] = (byte) ((int_ >> 8 * (3 - cap)) & 0xFF);
        }
        return bytes;
    }

    public static String lastIndexOf(String str, String ch, int num){
        for(int i = 0; i< num; i ++){
            int k = str.lastIndexOf(ch);
            if(k == -1){
                break;
            }
            str = str.substring(0,k);
        }
        return str;
    }
}
