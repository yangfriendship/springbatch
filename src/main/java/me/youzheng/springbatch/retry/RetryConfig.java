package me.youzheng.springbatch.retry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;

/**
 * retry 는 예외가 발생하면 chunk 를 다시 실행한다. 아래 예제를 실행하면 계속 예외가 반복하는 청크를 실행하여 결국 최대 반복 횟수에 도달한다.
 */
@Configuration
@RequiredArgsConstructor
@Profile("retry")
public class RetryConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .incrementer(new RunIdIncrementer())
            .start(step())
            .build();
    }

    @Bean
    public Step step() {
        return this.stepBuilderFactory.get("step")
            .<String, String>chunk(10)
            .reader(itemReader())
            .processor(itemProcessor())
            .writer(items -> System.out.println(items + " in writer"))
            .faultTolerant()
//            .retry(CustomRetryException.class)
//            .retryLimit(2)
            .skip(CustomRetryException.class)
            .skipLimit(10)
            .retryPolicy(policy())
            .build()
            ;
    }

    @Bean
    public RetryPolicy policy() {
        Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap<>();
        exceptionMap.put(CustomRetryException.class, true);
        return new SimpleRetryPolicy(2, exceptionMap);
    }

    @Bean
    public ItemProcessor<? super String, String> itemProcessor() {
        return new RetryItemProcessor();
    }

    @Bean
    public ItemReader<String> itemReader() {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            items.add("item" + (i + 1));
        }
        return new ListItemReader<>(items);
    }

    public class RetryItemProcessor implements ItemProcessor<String, String> {

        public RetryItemProcessor() {
            System.out.println("create!!");
        }

        @Override
        public String process(String item) throws Exception {
            if (Integer.parseInt(item.replace("item", "")) % 23 == 0) {
                System.out.println("Exception!!! :" + item + " : ");
                throw new CustomRetryException();
            }
            System.out.println(item);
            return item;
        }
    }

}
