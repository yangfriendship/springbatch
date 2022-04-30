package me.youzheng.springbatch.batch.job.api;


import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.batch.chunk.processor.ApiItemProcessor1;
import me.youzheng.springbatch.batch.chunk.processor.ApiItemProcessor2;
import me.youzheng.springbatch.batch.chunk.processor.ApiItemProcessor3;
import me.youzheng.springbatch.batch.chunk.writer.ApiItemWriter1;
import me.youzheng.springbatch.batch.chunk.writer.ApiItemWriter2;
import me.youzheng.springbatch.batch.chunk.writer.ApiItemWriter3;
import me.youzheng.springbatch.batch.classsifier.ProcessorClassifier;
import me.youzheng.springbatch.batch.classsifier.WriterClassifier;
import me.youzheng.springbatch.batch.domain.ApiRequestVo;
import me.youzheng.springbatch.batch.domain.ProductVo;
import me.youzheng.springbatch.batch.partition.ProductPartitioner;
import me.youzheng.springbatch.service.ApiService;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.support.ClassifierCompositeItemProcessor;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@RequiredArgsConstructor
public class ApiStepConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;
    private final ApiService apiService1;
    private final ApiService apiService2;
    private final ApiService apiService3;

    private final int chunkSize = 10;

    @Bean
    public Step apiMasterStep() throws Exception {
        ProductVo[] productList = QueryGenerator.getProductList(this.dataSource);
        return this.stepBuilderFactory.get("apiMasterStep")
            .partitioner(apiSlaveStep().getName(), partitioner())
            .step(apiSlaveStep())
            .gridSize(productList.length)    // 이거 빼먹어서 1시간 날림!
            .taskExecutor(taskExecutor())
            .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setMaxPoolSize(6);
        executor.setCorePoolSize(3);
        executor.setThreadNamePrefix("api-thread-");
        return executor;
    }

    @Bean
    public ProductPartitioner partitioner() {
        ProductPartitioner partitioner = new ProductPartitioner();
        partitioner.setDataSource(this.dataSource);
        return partitioner;
    }

    @Bean
    public Step apiSlaveStep() throws Exception {
        return this.stepBuilderFactory.get("apiSlaveStep")
            .<ProductVo, ProductVo>chunk(this.chunkSize)
            .reader(itemReader(null))
            .processor(itemProcessor())
            .writer(itemWriter())
            .build();
    }

    @Bean
    @StepScope
    public ItemReader<ProductVo> itemReader(@Value("#{stepExecutionContext['product']}") ProductVo productVo) throws Exception {

        JdbcPagingItemReader<ProductVo> reader = new JdbcPagingItemReader<>();

        reader.setDataSource(dataSource);
        reader.setPageSize(chunkSize);
        reader.setRowMapper(new BeanPropertyRowMapper(ProductVo.class));

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("id, name, price, type");
        queryProvider.setFromClause("from product");
        queryProvider.setWhereClause("where type = :type");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.DESCENDING);
        queryProvider.setSortKeys(sortKeys);

        reader.setParameterValues(QueryGenerator.getParameterForQuery("type", productVo.getType()));
        reader.setQueryProvider(queryProvider);
        reader.afterPropertiesSet();

        return reader;
    }

    @Bean
    public ItemProcessor itemProcessor() {

        ClassifierCompositeItemProcessor<ProductVo, ApiRequestVo> processor = new ClassifierCompositeItemProcessor<>();

        ProcessorClassifier<ProductVo, ItemProcessor<?, ? extends ApiRequestVo>> classifier = new ProcessorClassifier();

        Map<String, ItemProcessor<ProductVo, ApiRequestVo>> processorMap = new HashMap<>();
        processorMap.put("1", new ApiItemProcessor1());
        processorMap.put("2", new ApiItemProcessor2());
        processorMap.put("3", new ApiItemProcessor3());

        classifier.setMap(processorMap);

        processor.setClassifier(classifier);

        return processor;
    }

    @Bean
    public ItemWriter itemWriter() {

        ClassifierCompositeItemWriter<ApiRequestVo> writer = new ClassifierCompositeItemWriter<>();

        WriterClassifier<ApiRequestVo, ItemWriter<? super ApiRequestVo>> classifier = new WriterClassifier();

        Map<String, ItemWriter<ApiRequestVo>> writerMap = new HashMap<>();
        writerMap.put("1", new ApiItemWriter1(apiService1));
        writerMap.put("2", new ApiItemWriter2(apiService2));
        writerMap.put("3", new ApiItemWriter3(apiService3));

        classifier.setMap(writerMap);

        writer.setClassifier(classifier);

        return writer;
    }

}
