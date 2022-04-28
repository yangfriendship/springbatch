package me.youzheng.springbatch.partition;


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
import me.youzheng.springbatch.multithread.MultiThreadConfig;
import me.youzheng.springbatch.multithread.MultiThreadConfig.CustomerRowMapper;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.relational.core.sql.In;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * slaveStep 은 gridSize 만큼 생성된다. slaveStep 은 자신만의 stepExecutionContext 을 할당 받는다. ItemReader,
 * ItemWriter, ItemProcessor 은 각 slaveStep 마다 생성될 수 있도록
 * @StepScope 어노테이션을 사용하여 프록시 빈으로 만들어 실제 해당 객체가 호출될 때 생성되도록 만들어줘야한다.(Thread-safe)
 */
@Configuration
@RequiredArgsConstructor
@Profile("partition")
public class PartitionConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource dataSource;

    @Bean
    public Job batchJob() {
        return this.jobBuilderFactory.get("batchJob")
            .incrementer(new RunIdIncrementer())
            .listener(new StopWatchJobListener())
            .start(masterStep())
            .build();
    }

    @Bean
    public Step masterStep() {
        return this.stepBuilderFactory.get("masterStep")
            .partitioner(slaveStep().getName(), partitioner())
            .step(slaveStep()) // slave step bean
            .gridSize(4)    // slave step count
            .taskExecutor(taskExecutor())
            .build()
            ;
    }

    @Bean
    public Partitioner partitioner() {
        ColumnRangePartitioner partitioner = new ColumnRangePartitioner();
        partitioner.setColumn("id");
        partitioner.setTable("customer");
        partitioner.setDataSource(this.dataSource);
        return partitioner;
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);    // 생성할 스레드 갯수
        executor.setMaxPoolSize(8);     // 최대 스레드 갯수
        executor.setThreadNamePrefix("async-thread");
        executor.setQueueCapacity(32);
        return executor;
    }

    @Bean
    public Step slaveStep() {
        return this.stepBuilderFactory.get("slaveStep")
            .<Customer, Customer>chunk(1000)
            .reader(jdbcPagingItemReader())
            .writer(jdbcBatchWriter())
            .build();
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
    public CustomerRowMapper customerRowMapper() {
        return new CustomerRowMapper();
    }

    @StepScope
    @Bean
    public ItemReader<Customer> jdbcPagingItemReader() {
        return new JdbcPagingItemReaderBuilder<Customer>()
            .name("jdbcCursorItemReader")
            .fetchSize(1000)
            .rowMapper(customerRowMapper())
            .queryProvider(createQueryProvider(null, null))
            .dataSource(this.dataSource)
            .build()
            ;
    }


    @StepScope
    @Bean
    public PagingQueryProvider createQueryProvider(
        @Value("#{stepExecutionContext['minValue']}") Long minValue,
        @Value("#{stepExecutionContext['maxValue']}") Long maxValue) {
        System.out.println(String.format("%d ~ %d \n", minValue, maxValue));
        MySqlPagingQueryProvider factoryBean = new MySqlPagingQueryProvider();
        factoryBean.setSelectClause("id, firstName, lastName, birthdate");
        factoryBean.setFromClause("from customer");
        factoryBean.setWhereClause("where id >= " + minValue + " and id <= " + maxValue);
        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("id", Order.ASCENDING);
        factoryBean.setSortKeys(sortKey);
        try {
            return factoryBean;
        } catch (Exception e) {
            throw new RuntimeException("PagingQueryProvider 생성 실패");
        }
    }

    @StepScope
    @Bean
    public ItemWriter<? super Customer> jdbcBatchWriter() {
        return new JdbcBatchItemWriterBuilder<Customer>()
            .dataSource(this.dataSource)
            .sql("insert into customer2 values (:id, :firstName, :lastName, :birthdate)")
            .beanMapped()
            .build();
    }

}
