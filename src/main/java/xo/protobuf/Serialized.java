package xo.protobuf;

import xo.netty.codec.StudentPOJO.Student;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class Serialized {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoBuf.class);

    public static void main(String[] args) {
        Student student = Student.newBuilder()
                .setId(42)
                .setName("John")
                .build();

        // 序列化对象为字节流
        byte[] serializedData = student.toByteArray();

        // 将字节流写入文件
        try (FileOutputStream fos = new FileOutputStream("target/student.dat")) {
            fos.write(serializedData);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 从文件中读取字节流并反序列化为对象
        try (FileInputStream fis = new FileInputStream("target/student.dat")) {
            Student deserializedStudent = Student.parseFrom(fis);
            LOG.info("ID: " + deserializedStudent.getId());
            LOG.info("Name: " + deserializedStudent.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
