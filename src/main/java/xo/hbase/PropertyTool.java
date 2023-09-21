package xo.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyTool {
    private static final Logger LOG = LoggerFactory.getLogger(PropertyTool.class);

    public static Properties loadProperties(String fileName) {
        Properties properties = new Properties();
        try (InputStream inputStream = PropertyTool.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                throw new IOException("Unable to load the properties file: " + fileName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
