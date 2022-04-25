package me.youzheng.springbatch.db;

import java.util.HashMap;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.db.entity.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;

@RequiredArgsConstructor
@Configuration
@Profile("itemReader")
public class ItemReaderAdapterConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job job() {
        return this.jobBuilderFactory.get("batchJob")
            .start(step1())
            .incrementer(new RunIdIncrementer())
            .build();
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .<String, String>chunk(4)
            .reader(customReader())
            .writer(items -> {
                System.out.println("------------------------------------------");
                items.forEach(System.out::println);
            })
            .build()
            ;
    }

    @Bean
    public ItemReader<String> customReader() {
        ItemReaderAdapter<String> adapter = new ItemReaderAdapter<>();
        adapter.setTargetMethod("customRead");
        adapter.setTargetObject(customService());
        return adapter;
    }

    public static class CustomService<T> {

        private int count = 0;

        public T customRead() {
            if (count >= 100) {
                return null;
            }
            return (T) ("item" + (count++));
        }
    }

    @Bean
    public CustomService<String> customService() {
        return new CustomService<>();
    }

}
