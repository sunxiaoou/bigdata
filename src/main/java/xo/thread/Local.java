package xo.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 自定义用户信息类
class UserInfo {
    private int userId;
    private String username;

    public int getUserId() {
        return userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}

public class Local {
    private static final Logger LOG = LoggerFactory.getLogger(Local.class);

    // 自定义 ThreadLocal 变量
    private static final ThreadLocal<UserInfo> userInfoThreadLocal = ThreadLocal.withInitial(UserInfo::new);

    // 模拟线程池中的任务
    static class MyRunnable implements Runnable {
        private final int id;
        private final String name;

        public MyRunnable(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public void run() {
            UserInfo userInfo = new UserInfo();
            userInfo.setUserId(id);
            userInfo.setUsername(name);
            userInfoThreadLocal.set(userInfo);

            try {
                // 模拟执行一些业务逻辑
                LOG.info("Task is running with user: " + userInfoThreadLocal.get().getUsername());
                // ...
            } finally {
                // 在 finally 块中确保回收 ThreadLocal 变量
                userInfoThreadLocal.remove();
            }
        }
    }

    public static void main(String[] args) {
        // 创建线程池并提交任务
        Pool pool = new Pool("Worker");
        pool.execute(new MyRunnable(42, "john_doe"));
        pool.execute(new MyRunnable(43, "john_smith"));

        // 关闭线程池
        pool.shutdown();
    }
}