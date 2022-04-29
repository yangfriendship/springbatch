package me.youzheng.springbatch.web;

import java.util.Iterator;
import java.util.Set;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobController {

    @Autowired
    private JobRegistry jobRegistry;
    @Autowired
    private JobExplorer jobExplorer;
    @Autowired
    private JobOperator jobOperator;

    @PostMapping("/batch/start")
    public String start(@RequestBody JobInfo jobInfo)
        throws NoSuchJobException, JobInstanceAlreadyExistsException, JobParametersInvalidException {

        for (Iterator<String> iterator = this.jobRegistry.getJobNames().iterator();iterator.hasNext();) {
            SimpleJob job = (SimpleJob) jobRegistry.getJob(iterator.next());
            String jobName = job.getName();
            System.out.println("jobName = " + jobName);
            jobOperator.start(job.getName(), "id=" + jobInfo.getId());
        }

        return "batch is started";
    }

    @PostMapping("/batch/stop")
    public String stop()
        throws NoSuchJobException, NoSuchJobExecutionException, JobExecutionNotRunningException {

        for (Iterator<String> iterator = this.jobRegistry.getJobNames().iterator();
            iterator.hasNext(); ) {
            SimpleJob job = (SimpleJob) jobRegistry.getJob(iterator.next());

            Set<JobExecution> runningJobExecutions = this.jobExplorer.findRunningJobExecutions(
                job.getName());

            JobExecution jobExecution = runningJobExecutions.iterator().next();

            this.jobOperator.stop(jobExecution.getId());
        }

        return "batch is stopped";
    }

    @PostMapping("/batch/restart")
    public String restart()
        throws NoSuchJobException, JobParametersInvalidException, NoSuchJobExecutionException, JobExecutionNotRunningException, JobInstanceAlreadyCompleteException, JobRestartException {

        for (Iterator<String> iterator = this.jobRegistry.getJobNames().iterator();
            iterator.hasNext(); ) {
            SimpleJob job = (SimpleJob) jobRegistry.getJob(iterator.next());

            JobInstance lastJobInstance = jobExplorer.getLastJobInstance(job.getName());
            JobExecution jobExecution = jobExplorer.getLastJobExecution(lastJobInstance);
            this.jobOperator.restart(jobExecution.getId());
        }

        return "batch is stopped";
    }

}
