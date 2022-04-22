package me.youzheng.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("flowJob")
@Configuration
@RequiredArgsConstructor
public class FlowJobConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job flowJob() {
        return this.jobBuilderFactory.get("flowJob")
            .start(step1())
            .on("COMPLETED").to(successStep())
            .from(step1())
            .on("FAILED").to(failureStep())
            .end()
            .build()
            ;
    }

    @Bean
    public Step failureStep() {
        return this.stepBuilderFactory.get("failureStep")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("failureStep!");
                return null;
            }).build()
            ;
    }

    @Bean
    public Step successStep() {
        return this.stepBuilderFactory.get("failureStep")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("successStep!");
                return null;
            }).build()
            ;
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .tasklet((contribution, chunkContext) -> {
                throw new RuntimeException();
            }).build()
            ;
    }

}
