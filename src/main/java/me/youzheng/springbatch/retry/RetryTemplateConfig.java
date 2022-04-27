package me.youzheng.springbatch.retry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.db.entity.Customer;
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
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
@RequiredArgsConstructor
@Profile("retryTemplate")
public class RetryTemplateConfig {

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
            .<String, Customer>chunk(10)
            .reader(itemReader())
            .processor(itemProcessor())
            .writer(items -> {
                items.forEach(item -> System.out.println(item.getFirstName()));
            })
            .faultTolerant()
//            .retry(CustomRetryException.class)
//            .retryLimit(2)
            .skip(CustomRetryException.class)
            .skipLimit(10)
            .build()
            ;
    }

    @Bean
    public ItemProcessor<? super String, Customer> itemProcessor() {
        return new RetryItemProcessor(retryTemplate());
    }

    @Bean
    public ItemReader<String> itemReader() {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            items.add(String.valueOf(i));
        }
        return new ListItemReader<>(items);
    }

    public class RetryItemProcessor implements ItemProcessor<String, Customer> {

        private final RetryTemplate retryTemplate;

        public RetryItemProcessor(RetryTemplate retryTemplate) {
            this.retryTemplate = retryTemplate;
        }

        /**
         * retryTempate.execute 의 3번째 인자에 State 를 줄 수 있다.
         * State 가 없다면 retry 를 지정된 횟수만큼 진행한 후에 chunk 의 처음으로 돌아가는게 아니라
         * 바로 recovery 하는 과정으로 넘어간다. skip 과 chunk 처음으로 돌아가는 과정이 생략된다.
         ** */
        @Override
        public Customer process(String item) throws Exception {
            return retryTemplate.execute((RetryCallback<Customer, RuntimeException>) context -> {
                    if (item.equals("1") || item.equals("2")) {
                        throw new CustomRetryException("exception!! : " + item);
                    }
                    Customer result = new Customer();
                    result.setFirstName(item);
                    return result;
                }
                , context -> {
                    Customer result = new Customer();
                    result.setFirstName(item + ":recovered");
                    return null;
                });
        }
    }

    @Bean
    public RetryTemplate retryTemplate() {
        Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap<>();
        exceptionMap.put(CustomRetryException.class, true);
        SimpleRetryPolicy policy = new SimpleRetryPolicy(2, exceptionMap);

        FixedBackOffPolicy backOff = new FixedBackOffPolicy();
        backOff.setBackOffPeriod(2000);

        RetryTemplate template = new RetryTemplate();
        template.setRetryPolicy(policy);
//        template.setBackOffPolicy(backOff);
        return template;
    }

}
