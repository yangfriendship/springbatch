package me.youzheng.springbatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

//@Component
public class JobRunner implements ApplicationRunner {

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private Job job;

    /**
    * JobParameter 는 String, double, long, date 네 가지 타입을 받을 수 있다.
     * JobParameter 와 Job(JobName) 을 이용하여 JabInstance 를 생성
    * */
    @Override
    public void run(ApplicationArguments args) throws Exception {
        JobParameters parameter = new JobParametersBuilder()
            .addString("requestDate", "2")
            .toJobParameters();
        jobLauncher.run(job, parameter);
    }

}
