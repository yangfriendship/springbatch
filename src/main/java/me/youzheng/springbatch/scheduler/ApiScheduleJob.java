package me.youzheng.springbatch.scheduler;

import java.util.Date;
import lombok.SneakyThrows;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;


@Component
public class ApiScheduleJob extends QuartzJobBean {

    @Autowired
    private Job apiJob;
    @Autowired
    private JobLauncher jobLauncher;

    @SneakyThrows
    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        JobParameters parameters = new JobParametersBuilder()
            .addLong("id", new Date().getTime())
            .toJobParameters();
        this.jobLauncher.run(apiJob, parameters);
    }
}
