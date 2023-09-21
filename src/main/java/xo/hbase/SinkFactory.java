package xo.hbase;

import java.util.Properties;

public class SinkFactory {

    public static class Builder {
        final private String REPLICATE_SERVER_SINK_FACTORY = "replicate.server.sink.factory";
        private Properties properties;

        public AbstractSink build() throws Exception {
            ClassLoader cLoader = this.getClass().getClassLoader();
            Class<?> cls;
            try {
                String clsName = properties.getProperty(REPLICATE_SERVER_SINK_FACTORY);
                Class.forName(clsName);
                cls = cLoader.loadClass(clsName);
                return (AbstractSink) cls.getDeclaredConstructor(Properties.class).newInstance(properties);
            } catch(ClassNotFoundException e) {
                throw e;
            }
        }

        public Builder withConfiguration(Properties properties) {
            this.properties = properties;
            return this;
        }
    }

    private SinkFactory() {
    }
}