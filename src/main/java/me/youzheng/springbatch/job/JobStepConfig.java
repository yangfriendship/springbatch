package me.youzheng.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.job.DefaultJobParametersExtractor;
import org.springframework.batch.core.step.job.JobParametersExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("jobStep")
@Configuration
@RequiredArgsConstructor
public class JobStepConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job jab1() {
        return this.jobBuilderFactory.get("parentJob")
            .start(jobStep(null))
            .next(step2())
            .build();
    }

    @Bean
    public Step jobStep(JobLauncher jobLauncher) {
        return this.stepBuilderFactory.get("jobStep")
            .job(childJob())
            .launcher(jobLauncher)
            .parametersExtractor(jobParametersExtractor())
            .listener(new StepExecutionListener() {
                @Override
                public void beforeStep(StepExecution stepExecution) {
                    stepExecution.getExecutionContext().put("name", "user1");
                }

                @Override
                public ExitStatus afterStep(StepExecution stepExecution) {
                    return null;
                }
            })
            .build();
    }

    @Bean
    public JobParametersExtractor jobParametersExtractor() {
        DefaultJobParametersExtractor extractor = new DefaultJobParametersExtractor();
        extractor.setKeys(new String[]{"name"});
        return extractor;
    }

    @Bean
    public Job childJob() {
        return this.jobBuilderFactory.get("childJob")
            .start(step1())
            .build();
    }

    private Step step1() {
        return this.stepBuilderFactory
            .get("step1")
            .tasklet((contribution, chunkContext) -> null).build()
            ;
    }

    private Step step2() {
        return this.stepBuilderFactory
            .get("step2")
            .tasklet((contribution, chunkContext) -> null).build()
            ;
    }

}
