package xo.sap.jco;

import com.sap.conn.jco.JCoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class ODPDaemon {
    private static final Logger LOG = LoggerFactory.getLogger(ODPDaemon.class);
    private volatile boolean running = true;
    private final ODPWrapper odpWrapper;
    private final String subscriberType;
    private final String subscriberName;
    private final String subscriberProcess;
    private final List<String> tables;
    private final List<Thread> workers = new ArrayList<>();

    public ODPDaemon(String odpName,
                     Properties odpProperties,
                     String subscriberType,
                     String subscriberName,
                     String subscriberProcess,
                     List<String> tables) throws JCoException {
        this.odpWrapper = new ODPWrapper(odpName, odpProperties);
        this.subscriberType = subscriberType;
        this.subscriberName = subscriberName;
        this.subscriberProcess = subscriberProcess;
        this.tables = tables;
    }

    public void start() {
        for (String table: tables) {
            Thread worker = new Thread(() -> workerRun(table), "Worker-" + table);
            worker.start();
            workers.add(worker);
        }
    }

    private void workerRun(String table) {
        String[] split = table.split("\\.");
        String context = split[0];
        String queue = split[1];
        try {
            odpWrapper.resetODP(
                    subscriberType,
                    subscriberName,
                    subscriberProcess,
                    context,
                    queue
            );
            List<FieldMeta> fieldMetas = odpWrapper.getODPDetails(
                    subscriberType,
                    context,
                    queue).getThird();
            ODPParser odpParser = new ODPParser(queue, fieldMetas);
            while (running) {
                try {
                    List<byte[]> rows = odpWrapper.fetchODP(
                            subscriberType,
                            subscriberName,
                            subscriberProcess,
                            context,
                            queue,
                            "D");
                    if (!rows.isEmpty()) {
                        for (byte[] rowData : rows) {
                            //                        HexDump.hexDump(rowData);
                            LOG.info("row - {}", odpParser.parseRow2Json(rowData));
                        }
                    } else {
                        Thread.sleep(5000);
                    }
                } catch (InterruptedException e) {
                    // 如果线程被中断也可作为退出信号的一部分，这里简单忽略
                    Thread.currentThread().interrupt();
                }
            }
        } catch (JCoException e) {
            e.printStackTrace();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        LOG.info(Thread.currentThread().getName() + " exited");
    }

    /**
     * 等待特定的退出信号
     * 在真实场景中可以是：
     * - 接收到系统信号（如SIGTERM等），可通过Runtime.addShutdownHook来实现
     * - 等待用户在控制台输入某个命令（如输入exit后退出）
     * 此处简单用scanner等待用户敲回车来模拟
     */
    public void waitForStopSignal() {
        System.out.println("按回车键以停止所有工作线程...");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        scanner.close();
    }

    /**
     * 通知所有工作线程停止并等待它们退出
     */
    public void stopAndWaitWorkers() {
        // 设置running为false，通知线程结束
        running = false;

        // 等待所有工作线程结束
        for (Thread worker : workers) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                // 主线程等待过程中被打断，这里简单处理
                Thread.currentThread().interrupt();
            }
        }
    }

    private static Properties readProperties(String path) {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            properties.load(fis);
//            properties.forEach((key, value) -> System.out.println(key + ": " + value));
        } catch (IOException e) {
            LOG.error("Can not find file " + path, e);
        }
        return properties;
    }

    public static void main(String[] args) throws JCoException {
        ODPDaemon ODPDaemon = new ODPDaemon(
                "myTest",
                readProperties("ABAP_AS1.jcoDestination"),
                "RODPS_REPL_TEST",
                "TestRepository_DoesNotExist",
                "TestDataFlow_DoesNotExist",
                Arrays.asList("SLT~ODP01.FRUIT2"));
//                Arrays.asList("SLT~ODP01.FRUIT2", "SLT~ODP01.FRUIT3"));
        ODPDaemon.start();
        ODPDaemon.waitForStopSignal();
        ODPDaemon.stopAndWaitWorkers();
        LOG.info("All workers exited already");
    }
}