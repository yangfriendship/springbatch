package me.youzheng.springbatch.repeat;


import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.exception.SimpleLimitExceptionHandler;
import org.springframework.batch.repeat.policy.CompositeCompletionPolicy;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.batch.repeat.policy.TimeoutTerminationPolicy;
import org.springframework.batch.repeat.support.RepeatTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
* RepeatTemplate 를 종료시키는 3 가지 방법
 * 1. 반환하는 RepeatStatus
 * 2. ExceptionHandler
 * 3. CompletionPolicy
* */
@Profile("repeat")
@RequiredArgsConstructor
@Configuration
public class RepeatConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .start(step())
            .incrementer(new RunIdIncrementer())
            .build();
    }

    @Bean
    public Step step() {
        return this.stepBuilderFactory
            .get("step1")
            .<String, String>chunk(10)
            .reader(itemReader())
            .writer(itemWriter())
            .processor(itemProcessor())
            .build()
            ;
    }

    @Bean
    public ItemWriter<? super String> itemWriter() {
        return (ItemWriter<String>) items -> {
            System.out.println("==================================");
            items.forEach(System.out::println);
        };
    }

    @Bean
    public ItemReader<String> itemReader() {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            items.add("item" + i);
        }
        return new ListItemReader<>(items);
    }

    @Bean
    public ItemProcessor<String, String> itemProcessor() {
        RepeatTemplate repeatTemplate = new RepeatTemplate();
        return (ItemProcessor) item -> {
            // 1. chunkSize 만큼의 반복이 일어나면 종료한다.
//            repeatTemplate.setCompletionPolicy(new SimpleCompletionPolicy(2));
            // 2. 시간만큼 반복하고 종료한다.
//            repeatTemplate.setCompletionPolicy(new TimeoutTerminationPolicy(10));

            // 3. 여러가지 CompletionPolicy 를 조합하여 사용할 수 있다.
            CompositeCompletionPolicy completionPolicy = new CompositeCompletionPolicy();
            CompletionPolicy[] policies = new CompletionPolicy[]{
                new TimeoutTerminationPolicy(1000),
                new SimpleCompletionPolicy(100),
            };
            completionPolicy.setPolicies(policies);
            repeatTemplate.setCompletionPolicy(completionPolicy);

            repeatTemplate.setExceptionHandler(simpleLimitExceptionHandler());

            repeatTemplate.iterate(context -> {
                System.out.println("repeatTemplate: " + item);
                // 계속 반복을 하지만 CompletionPolicy 에 의해서 종료되도록 설정
                throw new RuntimeException();
//                return RepeatStatus.CONTINUABLE;
            });
            return item;
        };
    }

    /**
     * 꼭 빈으로 등록해서 사용해야한다. RepeatTemplate 를 만드는 과정에 setExceptionHandler(new ...) 로 만들면 정상적으로 작동하지
     * 않는다.
     */
    @Bean
    public SimpleLimitExceptionHandler simpleLimitExceptionHandler() {
        return new SimpleLimitExceptionHandler(100);
    }


}
