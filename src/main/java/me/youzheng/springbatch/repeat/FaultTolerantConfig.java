package me.youzheng.springbatch.repeat;


import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
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

@Profile("faultTolerant")
@RequiredArgsConstructor
@Configuration
public class FaultTolerantConfig {

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
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            items.add("item" + i);
        }
        return this.stepBuilderFactory
            .get("step1")
            .<String, String>chunk(10)
            .reader(new ItemReader<String>() {
                int i = 1;

                @Override
                public String read() {
                    if (i++ % 11 == 0) {
                        System.out.println(i-1);
                        throw new IllegalArgumentException();
                    }
                    return i >= items.size() ? null : items.get(i);
                }
            })
            .processor(new ItemProcessor<String, String>() {
                @Override
                public String process(String item) throws Exception {
//                    throw new IllegalStateException(item);
                    return item.toUpperCase(Locale.ROOT);
                }
            })
            .writer((item) -> System.out.println(item))
            .faultTolerant()
            .skip(IllegalArgumentException.class)
            .skipLimit(11)
            .retry(IllegalStateException.class)
            .retryLimit(2)
            .build()
            ;
    }


}
