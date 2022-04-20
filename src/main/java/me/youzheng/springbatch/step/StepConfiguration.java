package me.youzheng.springbatch.step;

import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.step.tasklet.CustomTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class StepConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    /**
     * 1. Job -> Step(in List) -> Tasklet 으로 호출 실행
     * */
    @Bean
    public Job job() {
        return jobBuilderFactory.get("taskletConfigJob")
            .start(step1())
            .next(step2())
            .next(step3())
            .next(step4())
            .build();
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .tasklet(new CustomTasklet(1)).build();
    }

    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("step2")
            .tasklet(new CustomTasklet(2)).build();
    }

    @Bean
    public Step step3() {
        return this.stepBuilderFactory.get("step3")
            .tasklet(new CustomTasklet(3)).build();
    }

    @Bean
    public Step step4() {
        return this.stepBuilderFactory.get("step4")
            .tasklet(new Tasklet() {
                @Override
                public RepeatStatus execute(StepContribution contribution,
                    ChunkContext chunkContext) throws Exception {
                    return RepeatStatus.FINISHED;
                }
            })
            .build();
    }
}
