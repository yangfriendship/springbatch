package me.youzheng.springbatch.listener;

import java.util.Collection;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JobRepositoryListener implements JobExecutionListener {

    @Autowired
    private JobRepository jobRepository;

    @Override
    public void beforeJob(JobExecution jobExecution) {

    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        JobParameters parameters = new JobParametersBuilder()
            .addString("requestDate", "1")
            .toJobParameters();
        /**
         * 1. 내부적으로 jobName+parameter 를 가지고 jobInstance 를 조회
         * 2. jobInstance 를 가지고 jobExecution 을 조회
         * 3. jobInstance 의 hashKey = jobName+parameters, jobExecution 의 pk 는 jobInstance_id
        */
        JobExecution execution = this.jobRepository.getLastJobExecution(jobName, parameters);
        if (execution == null) return;

        final Collection<StepExecution> stepExecutions = execution.getStepExecutions();
        for (StepExecution stepExecution : stepExecutions) {
            BatchStatus status = stepExecution.getStatus();
            System.out.println("status = " + status);
            ExitStatus exitStatus = stepExecution.getExitStatus();
            System.out.println("exitStatus = " + exitStatus);
            String stepName = stepExecution.getStepName();
            System.out.println("stepName = " + stepName);
        }

    }
}
