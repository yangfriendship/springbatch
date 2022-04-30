package me.youzheng.springbatch.batch.job.api;

import javax.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.batch.listener.jobListener;
import me.youzheng.springbatch.batch.tasklet.ApiEndTasklet;
import me.youzheng.springbatch.batch.tasklet.ApiStartTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class ApiJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final ApiStartTasklet apiStartTasklet;
    private final ApiEndTasklet apiEndTasklet;
    @Autowired
    private Step jobStep;

    @Bean
    public Job apiJob() {
        return this.jobBuilderFactory.get("apiJob")
            .listener(new jobListener())
            .start(apiStep1())
            .next(this.jobStep)
            .next(apiStep2())
            .build();
    }

    @Bean
    public Step apiStep2() {
        return this.stepBuilderFactory.get("step1")
            .tasklet(this.apiEndTasklet)
            .build();

    }

    @Bean
    public Step apiStep1() {
        return this.stepBuilderFactory.get("step2")
            .tasklet(this.apiStartTasklet)
            .build();

    }


}
