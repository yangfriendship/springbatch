package me.youzheng.springbatch.batch.job.file;

import javax.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.batch.chunk.processor.FileItemProcessor;
import me.youzheng.springbatch.batch.domain.Product;
import me.youzheng.springbatch.batch.domain.ProductVo;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@RequiredArgsConstructor
@Configuration
public class FileJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;

    @Bean
    public Job fileJob() {
        return this.jobBuilderFactory.get("fileJob")
            .incrementer(new RunIdIncrementer())
            .start(fileStep())
            .build();
    }

    @Bean
    public Step fileStep() {
        return this.stepBuilderFactory.get("fileStep")
            .<ProductVo, Product>chunk(10)
            .reader(fileItemReader(null))
            .processor(fileItemProcessor())
            .writer(fileItemWriter())
            .build();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<ProductVo> fileItemReader(
        @Value("#{jobParameters['requestDate']}") String requestDate) {
        return new FlatFileItemReaderBuilder<ProductVo>()
            .name("flatFileReader")
            .resource(new ClassPathResource("product_" + requestDate + ".csv"))
            .fieldSetMapper(new BeanWrapperFieldSetMapper<>())
            .targetType(ProductVo.class)
            .linesToSkip(1) // n 줄까지 skip
            .delimited().delimiter(",")
            .names("id", "name", "price", "type")
            .build();

    }

    @Bean
    public ItemProcessor<? super ProductVo, ? extends Product> fileItemProcessor() {
        return new FileItemProcessor();
    }

    @Bean
    public ItemWriter<Product> fileItemWriter() {
        return new JpaItemWriterBuilder<Product>()
            .entityManagerFactory(this.entityManagerFactory)
            .usePersist(true)
            .build();
    }
}
