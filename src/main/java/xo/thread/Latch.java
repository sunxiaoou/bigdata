package xo.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Latch {
    private static final Logger LOG = LoggerFactory.getLogger(Latch.class);

    public static void main(String[] args) {
        // 创建一个CountDownLatch，计数器初始值为3
        CountDownLatch latch = new CountDownLatch(3);

        // 创建并启动三个线程执行异步任务
        for (int i = 0; i < 3; i++) {
            Thread thread = new Thread(() -> {
                try {
                    LOG.info("执行异步任务");
                } catch (Exception e) {
                    // 处理异常
                } finally {
                    // 在退出前调用countDown方法
                    latch.countDown();
                }
            });
            thread.start();
        }

        try {
            // 主线程等待计数器变为0，最多等待一定的超时时间
            boolean completed = latch.await(5, TimeUnit.SECONDS);
            if (completed) {
                LOG.info("所有线程已完成异步任务。");
            } else {
                LOG.info("等待超时，未能等到所有线程完成。");
            }
        } catch (InterruptedException e) {
            // 处理等待中断异常
        }
    }
}

