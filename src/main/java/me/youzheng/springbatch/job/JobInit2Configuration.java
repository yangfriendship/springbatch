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


//@Configuration
public class JobInit2Configuration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final JobRepositoryListener jobRepositoryListener;

    public JobInit2Configuration(
        JobBuilderFactory jobBuilderFactory,
        StepBuilderFactory stepBuilderFactory,
        JobRepositoryListener jobRepositoryListener) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobRepositoryListener = jobRepositoryListener;
    }

    //@Bean
    public Job batchJob2() {
        return jobBuilderFactory.get("batchJob2")
            .incrementer(new RunIdIncrementer())
            .start(step21())
            .next(step22())
            .listener(this.jobRepositoryListener)
            .build();
    }

    //@Bean
    public Step step21() {
        return this.stepBuilderFactory.get("step1")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("Step2-1 is executed");
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

    //@Bean
    public Step step22() {
        return this.stepBuilderFactory.get("step2")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("Step2-2 is executed");
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
