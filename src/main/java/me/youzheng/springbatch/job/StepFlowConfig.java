package me.youzheng.springbatch.job;

import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.util.SimpleStepUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("stepFlow")
@Configuration
@RequiredArgsConstructor
public class StepFlowConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job flowStepJob() {
        SimpleJobBuilder batchJob = this.jobBuilderFactory.get("batchJob")
            .start(flowStep());
        return batchJob
            .incrementer(new RunIdIncrementer())
            .next(step2())
            .build();
    }

    @Bean
    public Step flowStep() {
        return this.stepBuilderFactory.get("flowStep")
            .flow(flow())
            .build();
    }

    @Bean
    public Flow flow() {
        FlowBuilder<Flow> builder = new FlowBuilder<>("flow");
        builder.start(step1())
            .end();
        return builder.build();
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .tasklet((contribution, chunkContext) -> {
                System.out.println(">>> step1");
                throw new RuntimeException("step1 fail");
//                return RepeatStatus.FINISHED;
            }).build();
    }

    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("step2")
            .tasklet((contribution, chunkContext) -> {
                System.out.println(">>> step2");
                return RepeatStatus.FINISHED;
            }).build();
    }

    @Bean
    public Step step3() {
        return this.stepBuilderFactory.get("step3")
            .tasklet((contribution, chunkContext) -> {
                System.out.println(">>> step3");
                return RepeatStatus.FINISHED;
            }).build();
    }

}
