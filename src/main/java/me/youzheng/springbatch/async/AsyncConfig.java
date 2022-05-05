package me.youzheng.springbatch.async;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.db.entity.Customer;
import me.youzheng.springbatch.listener.StopWatchJobListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.JobStepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

@Configuration
@RequiredArgsConstructor
@Profile("async")
public class AsyncConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource dataSource;

    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .start(step())
//            .start(asyncStep())
            .incrementer(new RunIdIncrementer())
            .listener(new StopWatchJobListener())
            .build();
    }

    @JobScope
    @Bean
    public Step step() {
        return this.stepBuilderFactory.get("step1")
            .<Customer, Customer>chunk(10)
            .reader(jdbcPagingItemReader())
            .processor(customProcessor())
            .writer(jdbcBatchWriter())
            .build()
            ;
    }

    @Bean
    public Step asyncStep() {
        return this.stepBuilderFactory.get("step1")
            .<Customer, Customer>chunk(10)
            .reader(jdbcPagingItemReader())
            .processor(asyncProcessor())
            .writer(asyncWriter())
            .build()
            ;
    }

    /**
    * AsyncItemWriter 는 ItemWirter 의 구현체를 호출하는 식으로 process() 를 처리한다.
    * */
    @Bean
    public AsyncItemWriter asyncWriter() {
        AsyncItemWriter<Customer> writer = new AsyncItemWriter<>();
        writer.setDelegate((ItemWriter<Customer>) this.jdbcBatchWriter());
        return writer;
    }

    /**
     * AsyncItemProcessor 는 ItemProcessor 의 구현체를 호출하는 식으로 process() 를 처리한다.
     * */
    @Bean
    public AsyncItemProcessor asyncProcessor() {
        AsyncItemProcessor<Customer, Customer> processor = new AsyncItemProcessor<>();
        processor.setDelegate(this.customProcessor());
        processor.setTaskExecutor(this.taskExecutor());
//        processor.afterPropertiesSet(); // 빈으로 설정하지 않는다면 설정
        return processor;
    }

    @Bean
    public SimpleAsyncTaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public ItemProcessor<Customer, Customer> customProcessor() {

        return new ItemProcessor<Customer, Customer>() {
            @Override
            public Customer process(Customer item) throws Exception {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e.getCause());
                }
                Customer customer = new Customer();
                customer.setFirstName(item.getFirstName().toUpperCase(Locale.ROOT));
                customer.setLastName(item.getLastName().toUpperCase(Locale.ROOT));
                customer.setBirthdate(item.getBirthdate().toUpperCase(Locale.ROOT));
                return customer;
            }
        };
    }

    public class CustomerRowMapper implements RowMapper<Customer> {

        @Override
        public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
            Customer customer = new Customer();
            customer.setFirstName(rs.getString("firstName"));
            customer.setLastName(rs.getString("lastName"));
            customer.setBirthdate(rs.getString("birthdate"));
            return customer;
        }
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

    @Bean
    public ItemWriter<? super Customer> jdbcBatchWriter() {
        return new JdbcBatchItemWriterBuilder<Customer>()
            .dataSource(this.dataSource)
            .sql("insert into customer2 values (:id, :firstName, :lastName, :birthdate)")
            .beanMapped()
            .build();
    }

}
