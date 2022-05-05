package me.youzheng.springbatch.job;

import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.util.SimpleStepUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.FlowJob;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("flowJobConfig")
@Configuration
@RequiredArgsConstructor
public class FlowJobConfig2 {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .incrementer(new RunIdIncrementer())
            .start(flow())
            .next(step3())
            .end()
            .build();
    }

    private Flow flow() {
        FlowBuilder<Flow> builder = new FlowBuilder("flow");
        builder.start(step1())
            .next(step2())
            .end();
        return builder.build();
    }

    @Bean
    public Step step1() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }

    @Bean
    public Step step2() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }

    @Bean
    public Step step3() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }


}
