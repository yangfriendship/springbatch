package me.youzheng.springbatch.job;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import me.youzheng.springbatch.listener.JobRepositoryListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class JobInitConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final JobRepositoryListener jobRepositoryListener;

    public JobInitConfiguration(
        JobBuilderFactory jobBuilderFactory,
        StepBuilderFactory stepBuilderFactory,
        JobRepositoryListener jobRepositoryListener) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobRepositoryListener = jobRepositoryListener;
    }

    @Bean
    public Job batchJob1() {
        return jobBuilderFactory.get("batchJob1")
            .incrementer(new RunIdIncrementer())
            .start(step1())
            .next(step2())
            .listener(this.jobRepositoryListener)
            .build();
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("Step1 is executed");
                Map<String, JobParameter> parameters = contribution.getStepExecution()
                    .getJobExecution().getJobParameters().getParameters();

                for (final Entry<String, JobParameter> entry : parameters.entrySet()) {
                    System.out.println(entry.getValue().getClass());
                    System.out.println(entry.getKey() + " : " + entry.getValue().getValue());
                }

                return RepeatStatus.FINISHED;
            }).build()
            ;
    }

    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("step2")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("Step2 is executed");
                Map<String, Object> jobParameters = chunkContext.getStepContext()
                    .getJobParameters();
                Set<Entry<String, Object>> entries = jobParameters.entrySet();
                for (Entry<String, Object> entry : entries) {
                    System.out.println(entry.getValue().getClass());
                    System.out.println(entry.getKey() + " : " + entry.getValue());
                }
                return RepeatStatus.FINISHED;
            }).build();
    }

}
