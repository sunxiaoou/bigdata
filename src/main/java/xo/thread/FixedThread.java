package xo.thread;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class FixedThread {

    // 是否继续运行的共享变量
    private volatile boolean running = true;

    // 所有队列
    private final List<BlockingQueue<String>> allQueues = new ArrayList<>();

    // 工作线程集合
    private final List<Thread> workers = new ArrayList<>();

    // 线程数
    private final int threadCount;

    public FixedThread(int threadCount) {
        this.threadCount = threadCount;
        // 假设有 7 个队列
        int queueNum = 7;
        for (int i = 0; i < queueNum; i ++) {
            LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
            // 模拟往队列中加入一些数据
            for (int j = 0; j < 3; j++) {
                queue.offer("Q" + i + " data " + j);
            }
            allQueues.add(queue);
        }
    }

    /**
     * 启动工作线程
     */
    public void start() {
        int queueCount = allQueues.size();  // M
        if (queueCount == 0 || threadCount <= 0) {
            System.out.println("无队列或线程数无效，直接返回");
            return;
        }

        // 如果 队列数 <= 线程数，直接“1:1”分配即可，
        // 多余的线程可以选择不创建，或者创建但没有分配队列。
        if (queueCount <= threadCount) {
            // 一个队列一个线程
            for (int i = 0; i < queueCount; i++) {
                BlockingQueue<String> queue = allQueues.get(i);
                Thread worker = new Thread(() -> workerRun(Collections.singletonList(queue)),
                        "Worker-" + i);
                worker.start();
                workers.add(worker);
            }
        } else {
            // 如果 队列数 > 线程数，需要把队列分组分配给每个线程
            // 这里采用最简单的平分思路
            int queueCountPerThread = (int)Math.ceil((double)queueCount / threadCount);

            int startIndex = 0;
            for (int i = 0; i < threadCount; i++) {
                int endIndex = Math.min(startIndex + queueCountPerThread, queueCount);
                // 子列表对应该线程负责的队列集合
                List<BlockingQueue<String>> subQueues = allQueues.subList(startIndex, endIndex);
                Thread worker = new Thread(() -> workerRun(subQueues), "Worker-" + i);
                worker.start();
                workers.add(worker);

                startIndex = endIndex;
                if (startIndex >= queueCount) {
                    break;
                }
            }
        }
    }

    /**
     * 工作线程逻辑：轮询多个队列
     */
    private void workerRun(List<BlockingQueue<String>> queues) {
        // 若该线程没有分配队列，也就直接退出
        if (queues.isEmpty()) {
            System.out.println(Thread.currentThread().getName() + " 未分配任何队列，退出。");
            return;
        }

        // 轮询队列时，可以选择“顺序轮询”或者“轮流轮询”
        // 这里演示最简单的顺序轮询
        while (running) {
            boolean hasProcessed = false;
            for (BlockingQueue<String> queue : queues) {
                if (!running) {
                    break;
                }
                try {
                    String data = queue.poll();
                    if (data != null) {
                        hasProcessed = true;
                        System.out.println(Thread.currentThread().getName()
                                + " 从队列处理数据: " + data);
                        // 省略实际数据处理逻辑
                    }
                } catch (Exception e) {
                    // 处理异常
                    e.printStackTrace();
                }
            }

            // 如果这一轮所有队列都没有取到数据，则可以稍作休眠
            if (!hasProcessed && running) {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    // 如果线程被中断，也可结束
                    Thread.currentThread().interrupt();
                }
            }
        }

        System.out.println(Thread.currentThread().getName() + " 已退出");
    }

    /**
     * 等待特定的退出信号
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
        // 设置running为false，通知线程退出
        running = false;
        // 等待所有线程结束
        for (Thread worker : workers) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) {
        // 假设指定固定线程数量
        int threadCount = 3;
        FixedThread daemon = new FixedThread(threadCount);

        // 启动线程
        daemon.start();

        // 模拟等待停止信号（回车）
        daemon.waitForStopSignal();

        // 通知所有工作线程停止并等待它们退出
        daemon.stopAndWaitWorkers();

        System.out.println("所有工作线程已结束，程序退出。");
    }
}