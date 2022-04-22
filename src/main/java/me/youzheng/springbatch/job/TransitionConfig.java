package me.youzheng.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("transition")
@Configuration
@RequiredArgsConstructor
public class TransitionConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .start(step1())
                .on("FAILED")
                .to(step2())
                .on("FAILED")
                .stop()
            .from(step1())
                .on("*")
                .to(step3())
                .next(step4())
            .from(step2())
                .on("*")
                .to(step5())
            .end()
            .build();
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .tasklet(((contribution, chunkContext) -> {
                System.out.println(
                    ">>> " + contribution.getStepExecution().getStepName() + " has executed!");
                return null;
            })).build();
    }

    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("step2")
            .tasklet(((contribution, chunkContext) -> {
                System.out.println(
                    ">>> " + contribution.getStepExecution().getStepName() + " has executed!");
                return null;
            })).build();
    }

    @Bean
    public Step step3() {
        return this.stepBuilderFactory.get("step3")
            .tasklet(((contribution, chunkContext) -> {
                System.out.println(
                    ">>> " + contribution.getStepExecution().getStepName() + " has executed!");
                return null;
            })).build();
    }

    @Bean
    public Step step4() {
        return this.stepBuilderFactory.get("step4")
            .tasklet(((contribution, chunkContext) -> {
                System.out.println(
                    ">>> " + contribution.getStepExecution().getStepName() + " has executed!");
                return null;
            })).build();
    }

    @Bean
    public Step step5() {
        return this.stepBuilderFactory.get("step5")
            .tasklet(((contribution, chunkContext) -> {
                System.out.println(
                    ">>> " + contribution.getStepExecution().getStepName() + " has executed!");
                return null;
            })).build();
    }

}
