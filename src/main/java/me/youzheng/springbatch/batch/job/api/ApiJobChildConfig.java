package me.youzheng.springbatch.batch.job.api;

import javax.batch.api.listener.JobListener;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class ApiJobChildConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    @Autowired
    private  Step apiMasterStep;
    private final JobLauncher jobLauncher;

    @Bean
    public Step jobStep() {
        return this.stepBuilderFactory.get("jobStep")
            .job(childJob())
            .launcher(this.jobLauncher)
            .build();
    }

    @Bean
    public Job childJob() {
        return this.jobBuilderFactory.get("childJob")
            .start(apiMasterStep)
            .build();
    }

}
