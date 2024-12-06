package xo.sap.jco;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.util.TypeUtils;
import javassist.*;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ODPParser {

    public static class FieldMeta {
        String name;
        int length;
        String type;

        public FieldMeta(String name, int length, String type) {
            this.name = name;
            this.length = length;
            this.type = type;
        }
    }

    private static int getUtf8BytesLength(byte[] data, int offset, int charCount) {
        int byteLength = 0; // 记录实际字节长度
        int charsProcessed = 0; // 已处理的 char 数量

        while (charsProcessed < charCount) {
            int currentByte = data[offset + byteLength] & 0xFF; // 当前字节的无符号值
            int charByteCount;

            // 判断当前字符的 UTF-8 字节数
            if ((currentByte & 0x80) == 0x00) { // 单字节字符
                charByteCount = 1;
            } else if ((currentByte & 0xE0) == 0xC0) { // 2 字节字符
                charByteCount = 2;
            } else if ((currentByte & 0xF0) == 0xE0) { // 3 字节字符
                charByteCount = 3;
            } else if ((currentByte & 0xF8) == 0xF0) { // 4 字节字符
                charByteCount = 4;
            } else {
                throw new IllegalArgumentException("Invalid UTF-8 encoding at byte offset " + (offset + byteLength));
            }

            // 验证续字节的合法性
            for (int i = 1; i < charByteCount; i++) {
                if ((data[offset + byteLength + i] & 0xC0) != 0x80) {
                    throw new IllegalArgumentException("Invalid UTF-8 continuation byte at offset " + (offset + byteLength + i));
                }
            }

            // 增加 char 计数
            if (charByteCount == 4) {
                charsProcessed += 2; // 补充字符对应两个 char
            } else {
                charsProcessed += 1; // 其他字符对应一个 char
            }

            // 增加字节偏移量
            byteLength += charByteCount;
        }

        return byteLength;
    }

    public static Object parseRow(byte[] data, List<FieldMeta> metaInfo) throws Exception {
        Class<?> pojoClass = generateDynamicClass(metaInfo);
        Object instance = pojoClass.getDeclaredConstructor().newInstance();

        int offset = 0;
        for (FieldMeta fieldMeta : metaInfo) {
            int length = fieldMeta.length;
            String fieldName = fieldMeta.name;
            String fieldType = fieldMeta.type;

            if ("CHAR".equalsIgnoreCase(fieldType)) {
                length = getUtf8BytesLength(data, offset, length);
            }
            String rawValue = new String(data, offset, length, StandardCharsets.UTF_8).trim();
            offset += length;

            Field field = pojoClass.getDeclaredField(fieldName);
            field.setAccessible(true);

            if ("INT4".equalsIgnoreCase(fieldType)) {
                field.set(instance, Integer.parseInt(rawValue));
            } else if ("CHAR".equalsIgnoreCase(fieldType)) {
                field.set(instance, rawValue);
            } else if ("FLTP".equalsIgnoreCase(fieldType)) {
                field.set(instance, Double.parseDouble(rawValue));
            } else if ("DEC".equalsIgnoreCase(fieldType)) {
                field.set(instance, Long.parseLong(rawValue));
            } else {
                field.set(instance, rawValue); // Default to String for unknown types
            }
        }

        return instance;
    }

    private static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private static Class<?> generateDynamicClass(List<FieldMeta> metaInfo) throws Exception {
        ClassPool pool = ClassPool.getDefault();
        CtClass ctClass = pool.makeClass("DynamicPOJO");

        for (FieldMeta fieldMeta : metaInfo) {
            String fieldType;
            switch (fieldMeta.type) {
                case "INT4":
                    fieldType = "int";
                    break;
                case "CHAR":
                    fieldType = "java.lang.String";
                    break;
                case "FLTP":
                    fieldType = "double";
                    break;
                case "DEC":
                    fieldType = "long";
                    break;
                default:
                    fieldType = "java.lang.String"; // Default to String for unknown types
            }

            // Add field
            CtField field = new CtField(pool.get(fieldType), fieldMeta.name, ctClass);
            field.setModifiers(Modifier.PRIVATE);
            ctClass.addField(field);

            // Add getter
            CtMethod getter = CtNewMethod.getter("get" + capitalize(fieldMeta.name), field);
            ctClass.addMethod(getter);

            // Add setter
            CtMethod setter = CtNewMethod.setter("set" + capitalize(fieldMeta.name), field);
            ctClass.addMethod(setter);
        }

        return ctClass.toClass();
    }

    public static String getJson(Object pojo) {
        TypeUtils.compatibleWithFieldName = true;
        return JSON.toJSONString(pojo);
    }

    static void test() {
        byte[] data = {(byte) 0xF0, (byte) 0x9F, (byte) 0x8D, (byte) 0x89, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20};
        System.out.println(getUtf8BytesLength(data, 0, 10));
    }

    public static void main(String[] args) throws Exception {
//        test();
//        System.exit(1);

        // Simulate metadata from RODPS_REPL_ODP_GET_DETAIL
        List<FieldMeta> metaInfo = new ArrayList<>();
        metaInfo.add(new FieldMeta("ID", 11, "INT4"));
        metaInfo.add(new FieldMeta("NAME", 10, "CHAR"));
        metaInfo.add(new FieldMeta("PRICE", 24, "FLTP"));
        metaInfo.add(new FieldMeta("ODQ_CHANGEMODE", 1, "CHAR"));
        metaInfo.add(new FieldMeta("ODQ_ENTITYCNTR", 20, "DEC"));

        // Simulate data from RODPS_REPL_ODP_FETCH
        byte[] data = {
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x31, 0x30, 0x31, 0x20, (byte) 0xF0, (byte) 0x9F, (byte) 0x8D, (byte) 0x89, 0x20,
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x36, 0x2E, 0x30, 0x30, 0x30, 0x30, 0x30,
                0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x45, 0x2B, 0x30, 0x32, 0x43,
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
                0x20, 0x20, 0x31, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        };

        Object pojo = parseRow(data, metaInfo);
        System.out.println(getJson(pojo));
    }
}
