package xo.fastjson;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.junit.Test;

import java.util.*;

public class TestFastJson {
    private Date getDate() {
        Date date = new Date(0);    // 1970-1-1
        return date;
    }

    @Test
    public void testObjToJson() {
        Student student = new Student();
        student.setId(1);
        student.setName("zhang3");
        student.setAge(50);
        student.setEmail("zs@163.com");
        student.setBirthday(getDate());

        String jsonString = JSON.toJSONString(student);
        System.out.println(jsonString);
        // {"age":50,"birthday":0,"email":"zs@163.com","id":1,"name":"zhang3"}
    }

    @Test
    public void testListToJson() {
        Student student = new Student();
        student.setId(1);
        student.setName("zhang3");
        student.setAge(50);
        student.setEmail("zs@163.com");
        student.setBirthday(getDate());

        Student student2 = new Student();
        student2.setId(2);
        student2.setName("li4");
        student2.setAge(52);
        student2.setEmail("ls@163.com");
        student2.setBirthday(getDate());

        List<Student> list = new ArrayList<>();
        list.add(student);
        list.add(student2);
        System.out.println(JSON.toJSONString(list));

        // [{"age":50,"birthday":0,"email":"zs@163.com","id":1,"name":"zhang3"},
        // {"age":52,"birthday":0,"email":"ls@163.com","id":2,"name":"li4"}]
    }

    @Test
    public void testMapToJson() {
        Student student = new Student();
        student.setId(1);
        student.setName("zhang3");
        student.setAge(50);
        student.setEmail("zs@163.com");
        student.setBirthday(getDate());

        Student student2 = new Student();
        student2.setId(2);
        student2.setName("li4");
        student2.setAge(52);
        student2.setEmail("ls@163.com");
        student2.setBirthday(getDate());

        Map<String, Student> map = new HashMap<>();
        map.put("student", student);
        map.put("student2", student2);
        System.out.println(JSON.toJSONString(map));

        // {"student":{"age":50,"birthday":0,"email":"zs@163.com","id":1,"name":"zhang3"},
        // "student2":{"age":52,"birthday":0,"email":"ls@163.com","id":2,"name":"li4"}}
    }

    @Test
    public void testJsonToObj() {
        String jsonString = "{\"age\":50,\"birthday\":0,\"email\":\"zs@163.com\",\"id\":1,\"name\":\"zhang3\"}";
        System.out.println(JSON.parseObject(jsonString, Student.class));
    }

    @Test
    public void testJsonToList() {
        String jsonString =
                "[{\"age\":50,\"birthday\":0,\"email\":\"zs@163.com\",\"id\":1,\"name\":\"zhang3\"}," +
                "{\"age\":52,\"birthday\":0,\"email\":\"ls@163.com\",\"id\":2,\"name\":\"li4\"}]";
        List<Student> list = JSON.parseArray(jsonString, Student.class);
//        System.out.println(list);
        for (Student student: list) {
            System.out.println(student);
        }
    }

    @Test
    public void testJsonToMap() {
        String jsonString =
                "{\"student\":{\"age\":50,\"birthday\":0,\"email\":\"zs@163.com\",\"id\":1,\"name\":\"zhang3\"}," +
                "\"student2\":{\"age\":52,\"birthday\":0,\"email\":\"ls@163.com\",\"id\":2,\"name\":\"li4\"}}";
        Map<String, Student> map = JSON.parseObject(jsonString, new TypeReference<Map<String, Student>>(){});
//        System.out.println(map);
        for (String key: map.keySet()) {
            System.out.println(key + "::" + map.get(key));
        }
    }
}
