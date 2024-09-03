package xo.quartz;

import org.junit.Test;
import org.quartz.SchedulerException;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class CronTabTest {

    @Test
    public void checkExpr() throws SchedulerException {
        List<String> expressions = Arrays.asList(
                "0 0 0,9 3,5,6,7 ? * *",
                "0 10 16 1 ? * *",
                "0 20,30 2 ? 11,24 * *",
                "0 0 0/2 * * * *"
        );
        int i = 0;
        for (String expression: expressions) {
            new CronTab(expression, "job" + i++, CronTab.MyJob.class);
        }
    }
}