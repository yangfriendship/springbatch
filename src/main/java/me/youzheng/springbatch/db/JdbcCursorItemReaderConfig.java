package me.youzheng.springbatch.db;

import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.db.entity.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("jdbcCursorItemReader")
@Configuration
@RequiredArgsConstructor
public class JdbcCursorItemReaderConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

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
            .reader(jdbcCursorItemReader())
            .writer(items -> {
                System.out.println("------------------------------------------");
                items.forEach(System.out::println);
            })
            .build()
            ;
    }

    @Bean
    public ItemReader<Customer> jdbcCursorItemReader() {
        return new JdbcCursorItemReaderBuilder<Customer>()
            .name("jdbcCursorItemReader")
            .fetchSize(2)
            .sql(
                "SELECT id, firstName, lastName, birthdate from customer where lastName like ? order by lastName, firstName;")
            .beanRowMapper(Customer.class)
            .queryArguments("y%")
            .dataSource(this.dataSource)
            .build()
            ;
    }

}
