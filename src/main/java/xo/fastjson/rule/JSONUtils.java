package xo.fastjson.rule;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.ValueFilter;

import java.util.Arrays;
import java.util.HashSet;

public class JSONUtils {
    public static String toJSONString(Object o) {
        JSONWriter writer = new JSONWriter();
        writer.writeObject(o);
        return writer.toString();
    }
    
    public static Object parse(String text) {
        JSONParser parser = new JSONParser(text);
        return parser.parse();
    }

    public static String removeSpecialChars(String str) {
        String newStr = str;
        if (newStr.startsWith("[\""))
            newStr = newStr.substring(2);
        if (newStr.endsWith("\"]"))
            newStr = newStr.substring(0, newStr.length()-2);
        return newStr;
    }

    public static HashSet<String> keysDeserveInt;
    public static HashSet<String> keysDeserveString;

    static {
        keysDeserveInt = new HashSet<>();
        keysDeserveInt.addAll(Arrays.asList("compress_switch","encrypt_switch","ziplevel"));

        keysDeserveString = new HashSet<>();
        keysDeserveString.addAll(Arrays.asList("bw_limit"));
    }

    public static JSONObject parseRuleWithDefaultValue(String ruleStr){
        JSONObject jsonObject = JSON.parseObject(ruleStr);
        String ruleStrNew = JSON.toJSONString(jsonObject, (ValueFilter) (object, key, value) -> {
            if (value == null){
                if(keysDeserveInt.contains(key)){
                    return 0;
                }else if(keysDeserveString.contains(key)){
                    return "";
                }
            }
            return value;
        });
        return JSON.parseObject(ruleStrNew);
    }

    public static <T> T parseRuleWithDefaultValue(String ruleStr, Class<T> clazz){
        T t = JSON.parseObject(ruleStr, clazz);
        String ruleStrNew = JSON.toJSONString(t, (ValueFilter) (object, key, value) -> {
            if (value == null){
                if(keysDeserveInt.contains(key)){
                    return 0;
                }else if(keysDeserveString.contains(key)){
                    return "";
                }
            }
            return value;
        });
        return JSON.parseObject(ruleStrNew,clazz);
    }

}
