package xo.fastjson.rule;

public class MsgConstants {

    private MsgConstants() { }

    //msg with iaback version number
    public static final byte MSG_VERSION = 2;

    //msg type from iaback
    public static final  byte MSG_O2K_HIS_START = 1;
    public static final  byte MSG_O2K_HIS_DATA  = MSG_O2K_HIS_START + 1;
    public static final  byte MSG_O2K_HIS_END    = MSG_O2K_HIS_START + 2;

    public static final  byte MSG_O2K_INCR_START = MSG_O2K_HIS_START + 3;
    public static final  byte  MSG_O2K_INCR_DML =  MSG_O2K_HIS_START + 4;
    public static final  byte  MSG_O2K_INCR_DDL = MSG_O2K_HIS_START + 5;
    public static final  byte  MSG_O2K_INCR_STOP  = MSG_O2K_HIS_START + 6;
    public static final  byte  MSG_O2K_INCR_RECON = MSG_O2K_HIS_START + 7;

    //msg type to iaback
    public static final  byte MSGT_PRO2BACK_STATE = 1;
    public static final  byte MSGT_PRO2BACK_TXNINFO = MSGT_PRO2BACK_STATE + 1;
    public static final  byte MSGT_PRO2BACK_ACK_HIS_END  = 3;

    //msg state code to iaback
    public static final int MSG_PRO2BACK_BASE = 1;
    public static final int MSG_PRO2BACK_QUEUE_FULL = MSG_PRO2BACK_BASE + 1;
    public static final int MSG_PRO2BACK_QUEUE_WRITABLE = MSG_PRO2BACK_BASE + 2;

    //msg from iaback format
    public static final  byte MSG_FMT_JSON = 1;
    public static final  byte MSG_AVRO = MSG_FMT_JSON + 1;
    public static final  byte MSG_PLAIN_TEXT  = MSG_FMT_JSON + 2;

    public static final int MAP_TAB = 0;
    public static final int MAP_USER = 1;
    public static final int MAP_DB = 2;
    public static final int MAP_MAX = 3;

    public static int getMapType(String typeName) {
        switch (typeName.toLowerCase()) {
            case "table":
                return MAP_TAB;
            case "user":
                return MAP_USER;
            case "db":
                return MAP_DB;
            default:
                return MAP_MAX;
        }
    }
}




