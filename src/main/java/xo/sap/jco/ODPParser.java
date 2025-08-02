package xo.sap.jco;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.util.TypeUtils;
import javassist.*;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ODPParser {
    private final List<FieldMeta> fieldMetas;
    private final Class<?> pojoClass;

    public ODPParser(String className, List<FieldMeta> fieldMetas) throws Exception {
        this.fieldMetas = fieldMetas;
        this.pojoClass = createPOJOClass(className);
    }

    private static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private Class<?> createPOJOClass(String className) throws Exception {
        ClassPool pool = ClassPool.getDefault();
        CtClass ctClass = pool.makeClass("Pojo" + className);

        for (FieldMeta fieldMeta : fieldMetas) {
            String fieldType;
            switch (fieldMeta.getType()) {
                case "INT4":
                    fieldType = "int";
                    break;
                case "INT8":
                    fieldType = "long";
                    break;
                case "CHAR":
                    fieldType = "java.lang.String";
                    break;
                case "FLTP":
                    fieldType = "double";
                    break;
                case "DEC":
                    fieldType = "java.math.BigDecimal";
                    break;
                default:
                    fieldType = "java.lang.String"; // Default to String for unknown types
            }

            // Add field
            CtField field = new CtField(pool.get(fieldType), fieldMeta.getName(), ctClass);
            field.setModifiers(Modifier.PRIVATE);
            ctClass.addField(field);

            // Add getter
            CtMethod getter = CtNewMethod.getter("get" + capitalize(fieldMeta.getName()), field);
            ctClass.addMethod(getter);

            // Add setter
            CtMethod setter = CtNewMethod.setter("set" + capitalize(fieldMeta.getName()), field);
            ctClass.addMethod(setter);
        }

        return ctClass.toClass();
    }

    public static int getUtf8BytesLength(byte[] data, int offset, int charCount) {
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

    public Object parseRow(byte[] data) throws Exception {
        Object instance = pojoClass.getDeclaredConstructor().newInstance();

        int offset = 0;
        for (FieldMeta fieldMeta : fieldMetas) {
            int length = fieldMeta.getOutputLength();
            String fieldName = fieldMeta.getName();
            String fieldType = fieldMeta.getType();

            if ("CHAR".equalsIgnoreCase(fieldType)) {
                length = getUtf8BytesLength(data, offset, length);
            }
            String rawValue = new String(data, offset, length, StandardCharsets.UTF_8).trim();
            offset += length;

            Field field = pojoClass.getDeclaredField(fieldName);
            field.setAccessible(true);

            if ("INT4".equalsIgnoreCase(fieldType) || "INT8".equalsIgnoreCase(fieldType) || "DEC".equalsIgnoreCase(fieldType)) {
                if (rawValue.endsWith("-")) {
                    rawValue = "-" + rawValue.substring(0, rawValue.length() - 1);
                }
            }
            if ("INT4".equalsIgnoreCase(fieldType)) {
                field.set(instance, Integer.parseInt(rawValue));
            } else if ("INT8".equalsIgnoreCase(fieldType)) {
                field.set(instance, Long.parseLong(rawValue));
            }
            else if ("CHAR".equalsIgnoreCase(fieldType)) {
                field.set(instance, rawValue);
            } else if ("FLTP".equalsIgnoreCase(fieldType)) {
                field.set(instance, Double.parseDouble(rawValue));
            } else if ("DEC".equalsIgnoreCase(fieldType)) {
                field.set(instance, new BigDecimal(rawValue));
            } else {
                field.set(instance, rawValue); // Default to String for unknown types
            }
        }

        return instance;
    }

    public String parseRow2Json(byte[] data) throws Exception {
        Object pojo = parseRow(data);
        TypeUtils.compatibleWithFieldName = true;
        return JSON.toJSONString(pojo);
    }
}
