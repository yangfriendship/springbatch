package me.youzheng.springbatch.multithread;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.db.entity.Customer;
import me.youzheng.springbatch.listener.StopWatchJobListener;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@RequiredArgsConstructor
@Profile("multithread")
public class MultiThreadConfig {

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
            .listener(new StopWatchJobListener())
//            .listener(customReaderListener())
            .processor(customProcessor())
//            .listener(customProcessorListener())
            .writer(jdbcBatchWriter())
//            .listener(customWriterListener())
//            .taskExecutor(taskExecutor())
            .build()
            ;
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);    // 생성할 스레드 갯수
        executor.setMaxPoolSize(8);     // 최대 스레드 갯수
        executor.setThreadNamePrefix("async-thread");
        return executor;
    }

    @Bean
    public ItemProcessListener<? super Customer, ? super Customer> customProcessorListener() {
        return new ItemProcessListener<Customer, Customer>() {
            @Override
            public void beforeProcess(Customer item) {

            }

            @Override
            public void afterProcess(Customer item, Customer result) {
                System.out.println(
                    "Thread: " + Thread.currentThread().getName() + " processor item: "
                        + item.getId());
            }

            @Override
            public void onProcessError(Customer item, Exception e) {

            }
        };
    }

    @Bean
    public ItemWriteListener<? super Customer> customWriterListener() {
        return new ItemWriteListener<Customer>() {
            @Override
            public void beforeWrite(List<? extends Customer> items) {

            }

            @Override
            public void afterWrite(List<? extends Customer> items) {
                System.out.println(
                    "Thread: " + Thread.currentThread().getName() + " read item: " + items.size());
            }

            @Override
            public void onWriteError(Exception exception, List<? extends Customer> items) {

            }
        };
    }

    @Bean
    public ItemReadListener<? super Customer> customReaderListener() {
        return new ItemReadListener<Customer>() {
            @Override
            public void beforeRead() {

            }

            @Override
            public void afterRead(Customer item) {
                System.out.println(
                    "Thread: " + Thread.currentThread().getName() + " read item: " + item.getId());
            }

            @Override
            public void onReadError(Exception ex) {

            }
        };
    }

    @Bean
    public ItemProcessor<Customer, Customer> customProcessor() {
        return new ItemProcessor<Customer, Customer>() {
            @Override
            public Customer process(Customer item) throws Exception {
                Customer customer = new Customer();
                customer.setId(item.getId());
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
            customer.setId(rs.getLong("id"));
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
            .fetchSize(500)
            .parameterValues(params)
            .rowMapper(new CustomerRowMapper())
            .queryProvider(createQueryProvider())
            .dataSource(this.dataSource)
            .build()
            ;
    }


    @Bean
    public PagingQueryProvider createQueryProvider() {
        MySqlPagingQueryProvider factoryBean = new MySqlPagingQueryProvider();
        factoryBean.setSelectClause("id, firstName, lastName, birthdate");
        factoryBean.setFromClause("from customer");
        factoryBean.setWhereClause("where lastName like :lastName");
        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("id", Order.ASCENDING);
        factoryBean.setSortKeys(sortKey);
        try {
            return factoryBean;
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
