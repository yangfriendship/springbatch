package me.youzheng.springbatch.scheduler;

import java.util.HashMap;
import lombok.RequiredArgsConstructor;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ApiJobRunner extends JobRunner {

    private final Scheduler schedulers;

    @Override
    void doRun(ApplicationArguments args) {
        JobDetail jobDetail = buildJobDetail(ApiScheduleJob.class, "apiJob", "batch",
            new HashMap());
        Trigger trigger = builderJobTrigger("0/30 * * * * ?");

        try {
            this.schedulers.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }

    }
}
