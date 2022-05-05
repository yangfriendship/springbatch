package me.youzheng.springbatch.job;

import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Profile("scope")
@Configuration
@RequiredArgsConstructor
public class ScopeConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    public static class JobListener implements JobExecutionListener {

        @Override
        public void beforeJob(JobExecution jobExecution) {
            jobExecution.getExecutionContext().put("name", "user1");
        }

        @Override
        public void afterJob(JobExecution jobExecution) {

        }
    }

    public static class StepListener implements StepExecutionListener {

        @Override
        public void beforeStep(StepExecution stepExecution) {
            stepExecution.getExecutionContext().put("UUID", UUID.randomUUID().toString());
            stepExecution.getExecutionContext().put("stepName", stepExecution.getStepName());
        }

        @Override
        public ExitStatus afterStep(StepExecution stepExecution) {
            return null;
        }
    }

    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .start(step1())
            .next(step2())
            .incrementer(new RunIdIncrementer())
            .listener(new JobListener())
            .build();
    }


    @Bean
    @JobScope
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .tasklet(tasklet(null, null))
//            .tasklet(tasklet())
            .listener(new StepListener())
            .build()
            ;
    }

    @Bean
    @JobScope
    public Step step2() {
        return this.stepBuilderFactory.get("step2")
            .tasklet(tasklet(null, null))
//            .tasklet(tasklet())
            .listener(new StepListener())
            .build()
            ;
    }

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{stepExecutionContext['stepName']}") String stepName,
        @Value("#{stepExecutionContext['UUID']}") String uuid) {
        Tasklet tasklet = new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
                throws Exception {
                System.out.println(">>> " + stepName + ": " + uuid);
                return null;
            }
        };
        return tasklet;
    }

//    @Primary
//    @Bean
//    public Tasklet tasklet() {
//        Tasklet kk = new Tasklet() {
//            @Override
//            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
//                throws Exception {
//                System.out.println("KK");
//                return null;
//            }
//        };
//        return kk;
//    }

}
