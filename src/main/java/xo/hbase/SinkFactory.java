package xo.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SinkFactory.class);

    public static class Builder {
        private ReplicateConfig config;

        public AbstractSink build() throws Exception {
            ClassLoader cLoader = this.getClass().getClassLoader();
            Class<?> cls;
            try {
                String clsName = config.getReplicateServerSinkFactory();
                LOG.info("Sink is " + clsName);
                Class.forName(clsName);
                cls = cLoader.loadClass(clsName);
                return (AbstractSink) cls.getDeclaredConstructor(ReplicateConfig.class).newInstance(config);
            } catch(ClassNotFoundException e) {
                throw e;
            }
        }

        public Builder withConfiguration(ReplicateConfig config) {
            this.config = config;
            return this;
        }
    }

    private SinkFactory() {
    }
}