package xo.protobuf;

import xo.netty.codec.StudentPOJO.Student;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.GeneratedMessageV3;

import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ProtoBufFile {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoBufFile.class);

    public static <T extends GeneratedMessageV3> void append(String filePath, T message) {
        try (FileOutputStream fos = new FileOutputStream(filePath, true)) {
            CodedOutputStream output = CodedOutputStream.newInstance(fos);

            int size = message.getSerializedSize();
            output.writeInt32NoTag(size);
            message.writeTo(output);

            output.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static <T extends GeneratedMessageV3> List<T> readAll(String filePath, Class<T> messageType) {
        List<T> messages = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(filePath)) {
            CodedInputStream input = CodedInputStream.newInstance(fis);

            while (!input.isAtEnd()) {
                int size = input.readInt32();
                byte[] data = input.readRawBytes(size);

                T message = null;
                try {
                    message = (T) messageType.getDeclaredMethod("parseFrom", byte[].class)
                            .invoke(null, data);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (message != null) {
                    messages.add(message);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return messages;
    }

    public static boolean deleteFile(String filePath) {
        File file = new File(filePath);
        return file.exists() && file.delete();
    }

    private static void test() {
        String path = "target/student.dat";
        deleteFile(path);

        Student student1 = Student.newBuilder().setId(42).setName("Alice").build();
        Student student2 = Student.newBuilder().setId(43).setName("Bob").build();
//        Student student1 = Student.newBuilder().setId(44).setName("Charles").build();
//        Student student2 = Student.newBuilder().setId(45).setName("Debby").build();

        append(path, student1);
        append(path, student2);

        List<Student> students = readAll(path, Student.class);
        for (Student student: students) {
            LOG.info("ID: " + student.getId());
            LOG.info("Name: " + student.getName());
        }
    }

    private static void test2() {
        String path = "target/entry.dat";
        for (EntryProto.Entry proto: readAll(path, EntryProto.Entry.class)) {
            WAL.Entry entry = ProtoBuf.proto2Entry(proto);
            LOG.info(entry.toString());
        }
    }

    public static void main(String[] args) {
//        test();
        test2();
    }
}
