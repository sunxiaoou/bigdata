package xo.fastjson;

import lombok.Data;

import java.util.Date;

@Data
public class Student {
    private Integer id;
    private String name;
    private Integer age;
    private Boolean isMale;
    private String email;
    private Date birthday;
}
