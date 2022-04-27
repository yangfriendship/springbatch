package me.youzheng.springbatch.repeat;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.relational.core.sql.In;

@Profile("skip")
@RequiredArgsConstructor
@Configuration
public class SkipConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .start(step())
            .incrementer(new RunIdIncrementer())
            .build();
    }

    public static class SkipItemProcessor implements ItemProcessor<String, String> {

        int count = 0;

        @Override
        public String process(String item) throws Exception {
            System.out.println(item + " in process");
            if (Integer.parseInt(item.replace("item", "")) % 27 == 0) {
                System.out.println("throw in Processor==================" + item);
                throw new SkippableException("Process failed");
            }
            return item.toUpperCase(Locale.ROOT);
        }

    }

    public static class SkippableException extends RuntimeException {

        public SkippableException(String message) {
            super(message);
        }
    }
    public static class SkipItemWriter implements ItemWriter<String> {

        @Override
        public void write(List<? extends String> items) throws Exception {
            for (String item : items) {
                if (Integer.parseInt(item.replace("ITEM", "")) % 13 == 0) {
                    System.out.println("throw in Writer==================" + item);
                    throw new SkippableException("Process failed");
                }
                System.out.println(item+ " in writer");
            }
            System.out.println("========Writer Finish===========");
        }
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
            .reader(new ListItemReader<>(items))
            .writer(this.skipItemWriter())
            .processor(this.skipItemProcessor())
            .faultTolerant()
//            .skip(SkippableException.class)
//            .skipLimit(3)
//            .skipPolicy(limitCheckingItemSkipPolicy())
            .skipPolicy(new AlwaysSkipItemSkipPolicy())
            .build()
            ;
    }

    @Bean
    public SkipPolicy limitCheckingItemSkipPolicy() {
        Map<Class<? extends Throwable>, Boolean> map = new HashMap<>();
        map.put(SkippableException.class, true);

        LimitCheckingItemSkipPolicy policy = new LimitCheckingItemSkipPolicy(3,map);

        return policy;
    }

    @Bean
    public ItemWriter<String> skipItemWriter() {
        return new SkipItemWriter();
    }

    @Bean
    public ItemProcessor<String, String> skipItemProcessor() {
        return new SkipItemProcessor();
    }

}
