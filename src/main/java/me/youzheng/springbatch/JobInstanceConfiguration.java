package me.youzheng.springbatch;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class JobInstanceConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job job() {
        return jobBuilderFactory.get("job")
            .start(step1())
            .next(step2())
            .build();
    }

    /**
     * contribution 를 통해서 JobParameters 에 접근할 수 있다.
     * contribution -> StepExecution -> JobExecution -> JobParameters
     * */
    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("Step1 is executed");
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


    /**
    * chunkContext 를 통해서도 JobParameter 에 접근 할 수 있다.
     * StepContext.getJobParameters 를 통해서 바로 Map<String,Object> 타입의 파라미터 키,값을 얻을 수 있다.
    * */
    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("step2")
            .tasklet((contribution, chunkContext) -> {
                System.out.println("Step2 is executed");
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

    /**
     * CommandLineArguments 로 JobParameter 을 넘길 수 있다.
     * {key}(type)={value} ex) rate(double)=0.2
     * date 의 구분자는 '/' ex) 2022/01/01
     * */

}
