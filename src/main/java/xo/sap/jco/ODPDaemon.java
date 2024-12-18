package xo.sap.jco;

import com.sap.conn.jco.JCoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ODPDaemon {
    private static final Logger LOG = LoggerFactory.getLogger(ODPDaemon.class);
    private volatile boolean running = true;
    private final ODPWrapper odpWrapper;
    private final String subscriberType;
    private final String subscriberName;
    private final String subscriberProcess;
    private final String context;
    private final List<String> queues;
    private final List<Thread> workers = new ArrayList<>();

    public ODPDaemon(ODPWrapper odpWrapper,
                     String subscriberType,
                     String subscriberName,
                     String subscriberProcess,
                     String context,
                     List<String> queues) {
        this.odpWrapper = odpWrapper;
        this.subscriberType = subscriberType;
        this.subscriberName = subscriberName;
        this.subscriberProcess = subscriberProcess;
        this.context = context;
        this.queues = queues;
    }

    public void start() {
        for (String queue: queues) {
            Thread worker = new Thread(() -> workerRun(queue), "Worker-" + queue);
            worker.start();
            workers.add(worker);
        }
    }

    private void workerRun(String queue) {
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

    public static void main(String[] args) throws JCoException {
        ODPDaemon ODPDaemon = new ODPDaemon(
                new ODPWrapper(DestinationConcept.SomeSampleDestinations.ABAP_AS1),
                "RODPS_REPL_TEST",
                "TestRepository_DoesNotExist",
                "TestDataFlow_DoesNotExist",
                "SLT~ODP01",
                Arrays.asList("FRUIT2", "FRUIT3"));
        ODPDaemon.start();
        ODPDaemon.waitForStopSignal();
        ODPDaemon.stopAndWaitWorkers();
        LOG.info("All workers exited already");
    }
}