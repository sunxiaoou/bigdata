package xo.sap.jco;

import java.util.HashMap;
import java.util.Properties;

import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.ext.DataProviderException;
import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;
import com.sap.conn.jco.ext.Environment;

/**
 * 自定义 DestinationDataProvider，允许直接在代码中写入 SAP 连接参数，而无需 .jcoDestination 文件。
 */
public class SimpleDestinationDataProvider implements DestinationDataProvider {

    private static SimpleDestinationDataProvider instance;
    private DestinationDataEventListener eventListener;
    private final HashMap<String, Properties> destinationStorage = new HashMap<>();

    // 单例模式，确保只有一个 DataProvider
    public static SimpleDestinationDataProvider getInstance() {
        if (instance == null) {
            instance = new SimpleDestinationDataProvider();
        }
        return instance;
    }

    private SimpleDestinationDataProvider() {}

    @Override
    public Properties getDestinationProperties(String destinationName) {
        Properties properties = destinationStorage.get(destinationName);
        if (properties == null) {
            throw new DataProviderException(DataProviderException.Reason.INVALID_CONFIGURATION, "Destination not found: " + destinationName, null);
        }
        return properties;
    }

    @Override
    public void setDestinationDataEventListener(DestinationDataEventListener eventListener) {
        this.eventListener = eventListener;
    }

    @Override
    public boolean supportsEvents() {
        return true;
    }

    /**
     * 允许动态添加或更新 SAP 目标配置
     */
    public void addOrUpdateDestination(String destinationName, Properties properties) {
        synchronized (destinationStorage) {
            destinationStorage.put(destinationName, properties);
            if (eventListener != null) {
                eventListener.updated(destinationName);
            }
        }
    }

    /**
     * 允许动态删除 SAP 目标配置
     */
    public void removeDestination(String destinationName) {
        synchronized (destinationStorage) {
            if (destinationStorage.remove(destinationName) != null && eventListener != null) {
                eventListener.deleted(destinationName);
            }
        }
    }

    /**
     * 注册此 `DestinationDataProvider` 到 JCo 环境
     */
    public static void register() {
        if (!Environment.isDestinationDataProviderRegistered()) {
            Environment.registerDestinationDataProvider(getInstance());
        }
    }

    /**
     * 直接添加 SAP 连接参数的便捷方法
     */
    public static void addDestination(String name, Properties properties) {
        getInstance().addOrUpdateDestination(name, properties);
    }

    public static void main(String[] args) {
        try {
            // 注册自定义 DestinationDataProvider
            SimpleDestinationDataProvider.register();
            // 直接添加 SAP 目标（无需 .jcoDestination 文件）
            Properties props = new Properties();
            props.setProperty(DestinationDataProvider.JCO_ASHOST, "sap01");
            props.setProperty(DestinationDataProvider.JCO_SYSNR, "01");
            props.setProperty(DestinationDataProvider.JCO_CLIENT, "150");
            props.setProperty(DestinationDataProvider.JCO_USER, "INFO2");
            props.setProperty(DestinationDataProvider.JCO_PASSWD, "Info1234@");
            props.setProperty(DestinationDataProvider.JCO_LANG, "en");
            SimpleDestinationDataProvider.addDestination("slt_as1", props);
            // 获取目标并测试连接
            JCoDestination destination = JCoDestinationManager.getDestination("slt_as1");
            destination.ping();
            System.out.println("Connection to ABAP_AS1 successful!");
        } catch (JCoException e) {
            e.printStackTrace();
            System.out.println("SAP connection failed.");
        }
    }
}
