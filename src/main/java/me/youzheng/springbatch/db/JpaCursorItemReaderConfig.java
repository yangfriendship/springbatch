package me.youzheng.springbatch.db;

import java.util.HashMap;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.db.entity.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("jpaCursorItemReader")
@Configuration
@RequiredArgsConstructor
public class JpaCursorItemReaderConfig {

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
            .<Customer, Customer>chunk(2)
            .reader(jpaCursorItemReader())
            .writer(items -> {
                System.out.println("------------------------------------------");
                items.forEach(System.out::println);
            })
            .build()
            ;
    }

    /**
     * JdbcCursorItemReader 와 다르게 InputStream.open() 에서 이미 데이터를 가져온다.
     */
    @Bean
    public ItemReader<Customer> jpaCursorItemReader() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("lastName", "yang");
        return new JpaCursorItemReaderBuilder<Customer>()
            .name("jdbcCursorItemReader")
            .queryString(
                "SELECT c from Customer c where lastName like :lastName")
            .parameterValues(parameters)
            .maxItemCount(10)
            .entityManagerFactory(entityManagerFactory)
            .currentItemCount(2)
            .build()
            ;
    }

}
