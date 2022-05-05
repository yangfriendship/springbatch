package me.youzheng.springbatch.operation;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("operation")
@RequiredArgsConstructor
public class JobOperationConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final JobRegistry jobRepository;

    @Bean
    public Job job() {
        return this.jobBuilderFactory.get("batchJob")
            .start(step1())
            .next(step2())
            .incrementer(new RunIdIncrementer())
            .build();
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .tasklet(new Tasklet() {
                @Override
                public RepeatStatus execute(StepContribution contribution,
                    ChunkContext chunkContext) throws Exception {
                    Thread.sleep(3000);
                    System.out.println("step1");
                    return RepeatStatus.FINISHED;
                }
            }).build()
            ;
    }

    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("step2")
            .tasklet(new Tasklet() {
                @Override
                public RepeatStatus execute(StepContribution contribution,
                    ChunkContext chunkContext) throws Exception {
                    Thread.sleep(3000);
                    System.out.println("step2");
                    return RepeatStatus.FINISHED;
                }
            }).build()
            ;
    }

    @Bean
    public BeanPostProcessor jobRegistryPostProcessor() {
        JobRegistryBeanPostProcessor processor = new JobRegistryBeanPostProcessor();
        processor.setJobRegistry(this.jobRepository);
        return processor;
    }


}
