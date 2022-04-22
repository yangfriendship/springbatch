package me.youzheng.springbatch.job;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("allowStart")
@Configuration
@RequiredArgsConstructor
public class AllowStartConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job taskletJob() {
        return this.jobBuilderFactory.get("allowStart")
            .start(step1())
            .next(step2())
//            .incrementer(parameters -> {
//                final String datetime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddhhmmss"));
//                return new JobParametersBuilder().addString("datetime", datetime).toJobParameters();
//            })
            .build();
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("taskletStep1")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("step1 in taskletStep1");
                return RepeatStatus.FINISHED;
            })
            .allowStartIfComplete(true)
            .build()
            ;
    }

    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("taskletStep2")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("step2 in taskletStep1");
                throw new RuntimeException();
            })
            .startLimit(3)
            .build()
            ;
    }

}
