package me.youzheng.springbatch.writer;

import java.util.HashMap;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.db.entity.Customer;
import me.youzheng.springbatch.db.entity.Customer2;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("jpaItemWriter")
@Configuration
@RequiredArgsConstructor
public class JpaItemWriterConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;

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
            .<Customer, Customer2>chunk(2)
            .reader(jpaCursorItemReader())
            .processor(customerProcessor())
            .writer(jpaItemWriter())
            .build()
            ;
    }

    @Bean
    public ItemProcessor<? super Customer, ? extends Customer2> customerProcessor() {
        return (ItemProcessor<Customer, Customer2>) item -> {
            Customer2 customer2 = new Customer2();
            customer2.setId(item.getId());
            customer2.setFirstName(item.getFirstName());
            customer2.setLastName(item.getLastName());
            customer2.setBirthdate(item.getBirthdate());
            return customer2;
        };
    }

    @Bean
    public ItemWriter<? super Customer2> jpaItemWriter() {
        return new JpaItemWriterBuilder<Customer2>()
            .usePersist(true)   // default = true
            .entityManagerFactory(this.entityManagerFactory)
            .build();
    }

    @Bean
    public ItemReader<Customer> jpaCursorItemReader() {
        return new JpaCursorItemReaderBuilder<Customer>()
            .name("jpaCursorItemReader")
            .queryString(
                "SELECT c from Customer c")
            .maxItemCount(10)
            .entityManagerFactory(entityManagerFactory)
            .currentItemCount(2)
            .build()
            ;
    }

}
