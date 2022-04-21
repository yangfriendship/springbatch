package me.youzheng.springbatch.job;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.validator.CustomParametersValidator;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Profile("jobParameterIncrement")
@Configuration
@RequiredArgsConstructor
public class JobParametersIncrementConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    public static class CustomParametersIncrement implements JobParametersIncrementer {

        static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(
            "yyyyMMdd-hhmmss");

        @Override
        public JobParameters getNext(JobParameters parameters) {
            String date = LocalDateTime.now().format(dateTimeFormatter);
            return new JobParametersBuilder().addString("date", date).toJobParameters();
        }
    }

    @Bean
    public JobParametersIncrementer jobParametersIncrementer() {
        return new CustomParametersIncrement();
    }

    @Bean
    public JobParametersValidator parameterValidator() {
        return new CustomParametersValidator();
    }

    @Bean
    public Job job() {
        return this.jobBuilderFactory.get("job1")
            .start(step1())
            .next(step2())
            .incrementer(jobParametersIncrementer())
            .build();
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory
            .get("step1")
            .tasklet(new Tasklet() {
                @Override
                public RepeatStatus execute(StepContribution contribution,
                    ChunkContext chunkContext) throws Exception {
                    System.out.println("step1");
                    return RepeatStatus.FINISHED;
                }
            })
            .build();
    }

    @Bean
    public Step step2() {
        return this.stepBuilderFactory
            .get("step2")
            .tasklet(new Tasklet() {
                @Override
                public RepeatStatus execute(StepContribution contribution,
                    ChunkContext chunkContext) throws Exception {
//                    throw new RuntimeException();
                    System.out.println("step2");
                    return RepeatStatus.FINISHED;
                }
            })
            .build();
    }

}
