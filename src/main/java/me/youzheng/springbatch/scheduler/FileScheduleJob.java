package me.youzheng.springbatch.scheduler;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;


public class FileScheduleJob extends QuartzJobBean {

    @Autowired
    private Job fileJob;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobExplorer jobExplorer;

    @SneakyThrows
    @Override
    protected void executeInternal(JobExecutionContext context) {

        String requestDate = (String) context.getJobDetail().getJobDataMap().get("requestDate");

        int instanceCount = jobExplorer.getJobInstanceCount(fileJob.getName());
        List<JobInstance> jobInstances = jobExplorer.getJobInstances(fileJob.getName(), 0,
            instanceCount);
        if (jobInstances.size() > 0) {
            for (JobInstance jobInstance : jobInstances) {
                List<JobExecution> jobExecutions = jobExplorer.getJobExecutions(jobInstance);
                List<JobExecution> filters = jobExecutions.stream().filter(
                        je -> je.getJobParameters().getString("requestDate").equals(requestDate))
                    .collect(Collectors.toList());
                if (filters.size() > 0) {
                    throw new JobExecutionException(requestDate + " already executed!!");
                }
            }
        }

        JobParameters parameters = new JobParametersBuilder()
            .addLong("id", new Date().getTime())
            .addString("requestDate", requestDate)
            .toJobParameters();

        this.jobLauncher.run(fileJob, parameters);
    }
}
