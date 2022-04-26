package me.youzheng.springbatch.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
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
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("compositeItemProcessor")
@RequiredArgsConstructor
@Configuration
public class CompositeItemProcessorConfig {

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
            .processor(compositeItemProcessor())
            .build()
            ;
    }

    private ItemWriter<? super String> itemWriter() {
        return (ItemWriter<String>) items -> {
            System.out.println("==================================");
            items.forEach(System.out::println);
        };
    }

    private ItemReader<String> itemReader() {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            items.add("item" + i);
        }
        return new ListItemReader<>(items);
    }

    private ItemProcessor<String, String> compositeItemProcessor() {
        List<ItemProcessor<String, String>> processors = new ArrayList<>();
        processors.add((item) -> item.toUpperCase(Locale.ROOT));
        processors.add((item) -> item + item);
        return new CompositeItemProcessorBuilder<String, String>()
            .delegates(processors)
            .build();
    }

}