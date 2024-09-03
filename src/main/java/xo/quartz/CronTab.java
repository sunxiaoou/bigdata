package xo.quartz;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CronTab {
    private static final Logger LOG = LoggerFactory.getLogger(CronTab.class);
    private final Scheduler scheduler;

    public static class MyJob implements Job {
        private static final Logger LOG = LoggerFactory.getLogger(MyJob.class);

        @Override
        public void execute(JobExecutionContext context) {
            LOG.info("Job is executing! Current time: " + System.currentTimeMillis());
        }
    }

    public CronTab(String cronExpr, String jobName, Class<? extends Job> jobClass) throws SchedulerException {
        scheduler = StdSchedulerFactory.getDefaultScheduler();
        JobDetail job = JobBuilder.newJob(jobClass)
                .withIdentity(jobName, "group1")
                .build();
        String quartzExpr = toQuartz(cronExpr);
        LOG.info("Convert expression({}) to ({})", cronExpr, quartzExpr);
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(jobName + "Trigger", "group1")
                .withSchedule(CronScheduleBuilder.cronSchedule(quartzExpr))
                .build();
        scheduler.scheduleJob(job, trigger);
    }

    private static String dowToQuartz(String dayOfMonth, String dayOfWeek) {
        if ("*".equals(dayOfMonth) && "*".equals(dayOfWeek)) {
            return "?";
        }
        return dayOfWeek
                .replaceAll("7", "0")       // Sunday
                .replaceAll("6", "7")       // Saturday
                .replaceAll("5", "6")       // Friday
                .replaceAll("4", "5")       // Thursday
                .replaceAll("3", "4")       // Wednesday
                .replaceAll("2", "3")       // Tuesday
                .replaceAll("1", "2")       // Monday
                .replaceAll("0", "1");      // Sunday
    }

    private static String toQuartz(String cronExpr) {
        // quartz - second, minute, hour, dayOfMonth, month, dayOfWeek
        String[] fields = cronExpr.trim().split("\\s+");
        if (fields.length == 6) {       // a quartz expression
            return cronExpr;
        }
        // cronTab - minute, hour, dayOfMonth, month, dayOfWeek
        if (fields.length == 5) {
            return String.join(" ", "0", fields[0], fields[1], fields[2], fields[3],
                    dowToQuartz(fields[2], fields[4]));
        }
        // offline - second, minute, hour, dayOfWeek, dayOfMonth, month, year
        assert fields.length == 7;
        return String.join(" ", fields[0], fields[1], fields[2], fields[4], fields[5],
                dowToQuartz(fields[4], fields[3]));
    }

    public void start() throws SchedulerException {
        scheduler.start();
    }

    public void stop() throws SchedulerException {
        scheduler.shutdown();
    }

    public static void main(String[] args) {
        try {
//            CronTab cronTab = new CronTab("0 * * ? * MON-FRI", MyJob.class);
            CronTab cronTab = new CronTab("* * ? * 1-5", "myJob", MyJob.class);
            cronTab.start();
            Thread.sleep(120000);
            cronTab.stop();
        } catch (SchedulerException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
