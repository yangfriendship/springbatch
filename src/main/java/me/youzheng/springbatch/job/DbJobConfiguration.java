package me.youzheng.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("dbJob")
@Configuration
@RequiredArgsConstructor
public class DbJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

//    @Bean
    public Job job() {
        return jobBuilderFactory.get("job")
            .start(step1())
            .next(step2())
            .build();
    }

//    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("Step1");
                for (int i = 0; i < 10000; i++) {
                    System.out.println(i);
                }
                return RepeatStatus.FINISHED;
            }).build()
            ;
    }

//    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("step2")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("Step2");
                return RepeatStatus.FINISHED;
            }).build()
            ;
    }

}
