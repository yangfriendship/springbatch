package me.youzheng.springbatch.job;

import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.util.SimpleStepUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("decider")
@Configuration
@RequiredArgsConstructor
public class JobExecutionDeciderConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .start(step1())
            .incrementer(new RunIdIncrementer())
            .next(decider())
            .from(decider()).on("ODD").to(oddStep())
            .from(decider()).on("EVEN").to(evenStep())
            .end().build();
    }

    public static int count = 1;

    /**
    * JobExecutionDecider 가 상태를 판단하여 새로운 ExecutionStatus 를 반환한다.
     * 실제 StepExecution 의 값을 변경하는게 아니라서 step1 의 상태는 Complete 로 반영된다.
    * */
    private JobExecutionDecider decider() {
        return (jobExecution, stepExecution) -> {
            count++;
            if (count % 2 == 0) {
                return new FlowExecutionStatus("EVEN");
            } else {
                return new FlowExecutionStatus("ODD");
            }
        };
    }

    @Bean
    public Step step1() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }

    @Bean
    public Step evenStep() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }

    @Bean
    public Step oddStep() {
        return SimpleStepUtils.createPrintStep(stepBuilderFactory).build();
    }


}
