package xo.quartz;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CronTab {
    private final Scheduler scheduler;

    public static class MyJob implements Job {
        private static final Logger LOG = LoggerFactory.getLogger(MyJob.class);

        @Override
        public void execute(JobExecutionContext context) {
            LOG.info("Job is executing! Current time: " + System.currentTimeMillis());
        }
    }

    public CronTab(String cronExpr, Class<? extends Job> jobClass) throws SchedulerException {
        scheduler = StdSchedulerFactory.getDefaultScheduler();
        JobDetail job = JobBuilder.newJob(jobClass)
                .withIdentity(jobClass.getSimpleName(), "group1")
                .build();
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("cronTrigger", "group1")
                .withSchedule(CronScheduleBuilder.cronSchedule(toQuartz(cronExpr)))
                .build();
        scheduler.scheduleJob(job, trigger);
    }

    private static String toQuartz(String cronExpr) {
        String[] fields = cronExpr.trim().split("\\s+");
        if (fields.length == 6) {       // a quartz expression
            return cronExpr;
        }
        assert fields.length == 5;
        String dayOfWeek = fields[4]
                .replaceAll("7", "0")       // Sunday
                .replaceAll("6", "7")       // Saturday
                .replaceAll("5", "6")       // Friday
                .replaceAll("4", "5")       // Thursday
                .replaceAll("3", "4")       // Wednesday
                .replaceAll("2", "3")       // Tuesday
                .replaceAll("1", "2")       // Monday
                .replaceAll("0", "1");      // Sunday
        return String.join(" ", "0", fields[0], fields[1], fields[2], fields[3], dayOfWeek);
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
            CronTab cronTab = new CronTab("* * ? * 1-5", MyJob.class);
            cronTab.start();
            Thread.sleep(120000);
            cronTab.stop();
        } catch (SchedulerException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
