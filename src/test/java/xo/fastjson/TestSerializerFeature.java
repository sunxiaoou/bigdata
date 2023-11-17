package xo.fastjson;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.junit.Test;

import java.util.Date;

public class TestSerializerFeature {
    private Date getDate() {
        Date date = new Date(0);    // 1970-1-1
        return date;
    }

    @Test
    public void testWriteNull() {
        Student student = new Student();
        student.setId(1);
        student.setName("zhang3");
        student.setAge(50);
//        student.setEmail("zs@163.com");
        student.setBirthday(getDate());
        System.out.println(JSON.toJSONString(student));
        System.out.println(JSON.toJSONString(student, SerializerFeature.WriteMapNullValue));
        System.out.println(JSON.toJSONString(student, SerializerFeature.WriteNullStringAsEmpty));
    }

    @Test
    public void testWriteNull2() {
        Student student = new Student();
        student.setId(1);
        student.setName("zhang3");
//        student.setAge(50);
//        student.setIsMale(true);
        student.setEmail("zs@163.com");
        student.setBirthday(getDate());
        System.out.println(JSON.toJSONString(student));
        System.out.println(JSON.toJSONString(student,
                SerializerFeature.WriteNullNumberAsZero, SerializerFeature.WriteNullBooleanAsFalse));
    }

    @Test
    public void testFormat() {
        Student student = new Student();
        student.setId(1);
        student.setName("zhang3");
        student.setAge(50);
        student.setIsMale(true);
        student.setEmail("zs@163.com");
        student.setBirthday(getDate());

        System.out.println(JSON.toJSONString(student));
        System.out.println(JSON.toJSONString(student,
                SerializerFeature.WriteDateUseDateFormat, SerializerFeature.PrettyFormat));
    }
}
