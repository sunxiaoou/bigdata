package xo.thread;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Daemon {
    private volatile boolean running = true;
    private final List<BlockingQueue<String>> queues;
    private final List<Thread> workers = new ArrayList<>();

    public Daemon(List<BlockingQueue<String>> queues) {
        this.queues = queues;
    }

    public void start() {
        for (int i = 0; i < queues.size(); i++) {
            BlockingQueue<String> queue = queues.get(i);
            Thread worker = new Thread(() -> workerRun(queue), "Worker-" + i);
            worker.start();
            workers.add(worker);
        }
    }

    /**
     * 工作线程逻辑，从指定队列中轮询数据，直到running为false
     */
    private void workerRun(BlockingQueue<String> queue) {
        while (running) {
            try {
                // poll() 非阻塞获取数据，也可以使用take()阻塞式获取
                String data = queue.poll();
                if (data != null) {
                    System.out.println(Thread.currentThread().getName() + " 处理数据: " + data);
                } else {
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                // 如果线程被中断也可作为退出信号的一部分，这里简单忽略
                Thread.currentThread().interrupt();
            }
        }
        // 线程即将退出时的清理或日志
        System.out.println(Thread.currentThread().getName() + " 已退出");
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

    public static void main(String[] args) {
        List<BlockingQueue<String>> queues = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
            queues.add(queue);
            // 塞入一些模拟数据
            for (int j = 0; j < 5; j++) {
                queue.offer("Queue " + i + " data " + j);
            }
        }

        Daemon daemon = new Daemon(queues);
        daemon.start();
        daemon.waitForStopSignal(); // 等待特定退出信号
        daemon.stopAndWaitWorkers(); // 通知工作线程结束并等待它们退出
        System.out.println("所有工作线程已结束，程序退出。");
    }
}