package xo.fastjson;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;
import org.junit.Test;

import java.util.Date;

public class TestAnnotation {
    @Data
    @JSONType(orders = {"studentName","age", "isMale", "birthday", "id"})
    static class StudentA {
        private Integer id;
        @JSONField(name = "studentName")
        private String name;
        private Integer age;
        private Boolean isMale;
        @JSONField(serialize = false)
        private String email;
        @JSONField(format = "YYYY-MM-dd")
        private Date birthday;
    }

    private Date getDate() {
        Date date = new Date(0);    // 1970-1-1
        return date;
    }

    @Test
    public void testObjToJson() {
        StudentA student = new StudentA();
        student.setId(1);
        student.setName("zhang3");
        student.setAge(50);
        student.setIsMale(true);
        student.setEmail("zs@163.com");
        student.setBirthday(getDate());

        String jsonString = JSON.toJSONString(student);
        System.out.println(jsonString);
        // {"age":50,"birthday":0,"email":"zs@163.com","id":1,"isMale":true,"name":"zhang3"}
        // {"studentName":"zhang3","age":50,"isMale":true,"birthday":"1970-01-01","id":1}
    }
}
