package me.youzheng.springbatch.scheduler;

import java.util.HashMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Component;

@Component
public class FileJobRunner extends JobRunner {

    private final Scheduler schedulers;

    public FileJobRunner(Scheduler schedulers) {
        this.schedulers = schedulers;
    }

    @Override
    void doRun(ApplicationArguments args) {

        String[] sourceArgs = args.getSourceArgs();

        JobDetail jobDetail = buildJobDetail(FileScheduleJob.class, "fileJob", "batch",
            new HashMap());
        Trigger trigger = builderJobTrigger("0/10 * * * * ?");

        // null 체크 해야된다.
        jobDetail.getJobDataMap().put("requestDate", sourceArgs[0]);
        try {
            this.schedulers.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }
}
