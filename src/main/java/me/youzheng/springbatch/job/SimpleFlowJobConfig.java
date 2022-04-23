package me.youzheng.springbatch.job;

import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.util.SimpleStepUtils;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("simpleFlow")
@Configuration
@RequiredArgsConstructor
public class SimpleFlowJobConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    /**
    * batchJob 의 BatchStatus 는 Completed 지만, ExitStatus 는 Failed
    * */
    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .incrementer(new RunIdIncrementer())
            .start(flow1())
                .on("COMPLETED").to(flow2())
            .from(flow1())
                .on("FAILED").to(flow3())
            .end()
            .build();
    }

    @Bean
    public Flow flow1() {
        FlowBuilder<Flow> builder = new FlowBuilder("flow1");
        builder.start(step1())
            .next(step2())
            .end();
        return builder.build();
    }

    @Bean
    public Flow flow2() {
        FlowBuilder<Flow> builder = new FlowBuilder("flow2");
        builder.start(flow3())
            .next(step5())
            .next(step6())
            .end();
        return builder.build();
    }

    @Bean
    public Flow flow3() {
        FlowBuilder<Flow> builder = new FlowBuilder("flow3");
        builder.start(step3())
            .next(step4())
            .end();
        return builder.build();
    }

    @Bean
    public Step step1() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }

    @Bean
    public Step step2() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory, ExitStatus.FAILED).build();
    }

    @Bean
    public Step step3() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }

    @Bean
    public Step step4() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }

    @Bean
    public Step step5() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }

    @Bean
    public Step step6() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }


}
