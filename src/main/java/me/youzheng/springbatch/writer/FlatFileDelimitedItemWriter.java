package me.youzheng.springbatch.writer;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.reader.flatfile.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Profile("FlatFileDelimitedItemWriter")
@RequiredArgsConstructor
@Configuration
public class FlatFileDelimitedItemWriter {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job job() {
        return this.jobBuilderFactory.get("batchJob")
            .incrementer(new RunIdIncrementer())
            .start(step1())
            .build();
    }

    private Step step1() {
        return this.stepBuilderFactory.get("step1")
            .<Object, Customer>chunk(3)
            .writer(flatFileDelimitedItemWriter())
            .reader(flatFileDelimitedItemReader())
            .build()
            ;
    }

    private ItemReader<?> flatFileDelimitedItemReader() {
        List<Customer> result = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            result.add(new Customer("name" + i, i, 2000 + i));
        }
        ItemReader<Customer> customerListItemReader = new ListItemReader<>(result);
        return customerListItemReader;
    }

    private ItemWriter<? super Customer> flatFileDelimitedItemWriter() {
        return new FlatFileItemWriterBuilder<>()
            .name("flatFileWriter")
            .resource(new FileSystemResource(
                "/Users/youzheng/workspace/study/batch/springbatch/src/main/resources/dist/customer.txt"))
            .shouldDeleteIfEmpty(true) // 데이터가 없다면 스킵!
            .append(true)   // 기존 파일에 이어서 쓰도록 설정!
            .delimited().delimiter("|")
            .names(new String[]{"name", "year", "age"})
            .build();
    }

}
