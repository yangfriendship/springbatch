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

@Profile("tasklet")
@Configuration
@RequiredArgsConstructor
public class TaskletConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    public static class CustomParametersIncrement implements JobParametersIncrementer {

        static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(
            "yyyyMMdd-hhmmss");

        @Override
        public JobParameters getNext(JobParameters parameters) {
            String date = LocalDateTime.now().format(dateTimeFormatter);
            return new JobParametersBuilder(parameters).addString("dateTime", date).toJobParameters();
        }
    }

    static class CustomTasklet implements Tasklet {

        @Override
        public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
            throws Exception {
            String jobName = chunkContext.getStepContext().getJobName();
            System.out.println("Custom Task: jobName = " + jobName);
            String stepName = chunkContext.getStepContext().getStepName();
            System.out.println("Custom Task: stepName = " + stepName);
            return RepeatStatus.FINISHED;
        }
    }

    @Bean
    public Tasklet customTasklet() {
        return new CustomTasklet();
    }

    @Bean
    public Job taskletJob() {
        return this.jobBuilderFactory.get("taskletConfigJob")
            .start(step1())
            .next(chunkStep())
            .incrementer(new CustomParametersIncrement())
            .build();
    }

    private Step step1() {
        return this.stepBuilderFactory.get("taskletStep1")
            .tasklet(this.customTasklet())
            .build()
            ;
    }

    @Bean
    public Step chunkStep() {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            items.add("item" + (i + 1));
        }
        return this.stepBuilderFactory.get("chunkStep")
            .<String, String>chunk(10)
            .reader(new ListItemReader<>(items))
            .processor(new ItemProcessor<String, String>() {
                @Override
                public String process(String item) throws Exception {
                    return item.toUpperCase(Locale.ROOT);
                }
            })
            .writer(new ItemWriter<String>() {
                @Override
                public void write(List<? extends String> items) throws Exception {
                    items.forEach(System.out::println);
                }
            }).build();
    }
}
