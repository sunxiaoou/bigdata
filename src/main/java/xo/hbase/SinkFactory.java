package xo.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SinkFactory.class);

    public static class Builder {
        private ReplicateConfig config;

        public AbstractSink build() {
            ClassLoader cLoader = this.getClass().getClassLoader();
            Class<?> cls;
            try {
                String clsName = config.getReplicateServerSink();
                if (clsName == null) {
                    clsName = "xo.hbase.DummySink";
                }
                LOG.info("Sink is " + clsName);
                Class.forName(clsName);
                cls = cLoader.loadClass(clsName);
                return (AbstractSink) cls.getDeclaredConstructor(ReplicateConfig.class).newInstance(config);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
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