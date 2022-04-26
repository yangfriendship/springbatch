package me.youzheng.springbatch.writer;

import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.db.entity.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

@Profile("jdbcBatchWriter")
@Configuration
@RequiredArgsConstructor
public class JdbcBatchWriterConfig {

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
            .<Customer, Customer>chunk(10)
            .reader(jdbcPagingItemReader())
            .writer(jdbcBatchWriter())
            .build()
            ;
    }

    @Bean
    public ItemWriter<? super Customer> jdbcBatchWriter() {
        return new JdbcBatchItemWriterBuilder<Customer>()
            .dataSource(this.dataSource)
            .sql("insert into customer2 values (:id, :firstName, :lastName, :birthdate)")
            .beanMapped()
            .build();
    }

    @Bean
    public ItemReader<Customer> jdbcPagingItemReader() {
        Map<String, Object> params = new HashMap<>();
        params.put("lastName", "y%");
        return new JdbcPagingItemReaderBuilder<Customer>()
            .name("jdbcCursorItemReader")
            .fetchSize(10)
            .rowMapper(new BeanPropertyRowMapper<>(Customer.class))
            .queryProvider(createQueryProvider())
            .parameterValues(params)
            .dataSource(this.dataSource)
            .build()
            ;
    }

    /**
     * queryProvider 를 통해서 query 를 생성한다.
     */
    @Bean
    public PagingQueryProvider createQueryProvider() {
        SqlPagingQueryProviderFactoryBean factoryBean = new SqlPagingQueryProviderFactoryBean();
        factoryBean.setDataSource(this.dataSource);
        factoryBean.setSelectClause("id, firstName, lastName, birthdate");
        factoryBean.setFromClause("from customer");
        factoryBean.setWhereClause("where lastName like :lastName");
        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("id", Order.ASCENDING);
        factoryBean.setSortKeys(sortKey);
        try {
            return factoryBean.getObject();
        } catch (Exception e) {
            throw new RuntimeException("PagingQueryProvider 생성 실패");
        }
    }

}
