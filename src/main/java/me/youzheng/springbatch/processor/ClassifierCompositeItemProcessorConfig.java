package me.youzheng.springbatch.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ClassifierCompositeItemProcessor;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.builder.ClassifierCompositeItemProcessorBuilder;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.classify.Classifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.relational.core.sql.In;

/**
 * Classifier 로 라우팅 배턴을 구현, ItemProcessor 구현체 중 하나를 호출하는 역할을 한다.
 */
@Profile("classifierCompositeItemProcessor")
@RequiredArgsConstructor
@Configuration
public class ClassifierCompositeItemProcessorConfig {

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
            .<ProcessInfo, ProcessInfo>chunk(10)
            .reader(itemReader())
            .writer(itemWriter())
            .processor(compositeItemProcessor())
            .build()
            ;
    }

    private ItemWriter<ProcessInfo> itemWriter() {
        return items -> {
            System.out.println("==================================");
            items.forEach(System.out::println);
        };
    }

    private ItemReader<ProcessInfo> itemReader() {
        List<ProcessInfo> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            items.add(new ProcessInfo(i));
        }
        return new ListItemReader<>(items);
    }

    private ItemProcessor<ProcessInfo, ProcessInfo> compositeItemProcessor() {
        ClassifierCompositeItemProcessor<ProcessInfo, ProcessInfo> processor = new ClassifierCompositeItemProcessor<>();

        Map<Integer, ItemProcessor<ProcessInfo, ProcessInfo>> map = new HashMap<>();

        /**
         * 숫자가 짝수이면 그대로 반환
         * 숫자가 홀수이면 음수 처리한다.
         * */
        map.put(0, item -> item);
        map.put(1, item -> {
            item.setId(item.getId() * -1);
            return item;
        });

        ProcessorClassifier<ProcessInfo, ItemProcessor<?, ? extends ProcessInfo>> classifier
            = new ProcessorClassifier<>();
        classifier.setMap(map);

        processor.setClassifier(classifier);
        return processor;
    }

    public static class ProcessorClassifier<C, T> implements Classifier<C, T> {

        @Setter
        public Map<Integer, ItemProcessor<ProcessInfo, ProcessInfo>> map;

        @Override
        public T classify(C classifiable) {
            return (T) this.map.get(((ProcessInfo) classifiable).getId() % 2);
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class ProcessInfo {

        private int id;

        @Override
        public String toString() {
            return "ProcessInfo{" +
                "id=" + id +
                '}';
        }
    }

}