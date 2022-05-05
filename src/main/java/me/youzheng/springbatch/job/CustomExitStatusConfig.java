package me.youzheng.springbatch.job;

import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.listener.PassCheckingListener;
import me.youzheng.springbatch.util.SimpleStepUtils;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("exitStatus")
@Configuration
@RequiredArgsConstructor
public class CustomExitStatusConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .start(step1())
                .on("FAILED")
                .to(step2())
                .on("PASS") // step2 가 PASS 가 아니라면 JobStatus,ExitStatus 는 FAILED 가 된다.
                .stop()
            .end()
            .build();
    }

    @Bean
    public Step step1() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory, ExitStatus.FAILED).build();
    }

    @Bean
    public Step step2() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory)
            .listener(new PassCheckingListener())
            .build();
    }

    @Bean
    public Step step3() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }

}
