package me.youzheng.springbatch.reader.flatfile;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;

@Profile("fixedLengthTokenizer")
@Configuration
@RequiredArgsConstructor
public class FixedLengthTokenizerConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job job() {
        return this.jobBuilderFactory.get("batchJob")
            .incrementer(new RunIdIncrementer())
            .start(step1())
            .build();
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("step1")
            .<Object, Customer>chunk(5)
//            .reader(customFlatFileItemReader())
            .reader(flatFileItemReader())
            .writer(new ItemWriter<Customer>() {
                @Override
                public void write(List<? extends Customer> items) throws Exception {
                    System.out.println("items = " + items);
                }
            })
            .build();
    }

    @Bean
    public ItemReader<Customer> flatFileItemReader() {
        return new FlatFileItemReaderBuilder<Customer>()
            .name("fixedLengthTokenizerFlatFileReader")
            .resource(new ClassPathResource("/customer.txt"))
            .fieldSetMapper(new BeanWrapperFieldSetMapper<>())
            .targetType(Customer.class)
            .linesToSkip(1)
            .fixedLength()
            /**
            * Range 를 설정한다.
            * */
            .addColumns(new Range(1, 8))
            .addColumns(new Range(9, 10))
            .addColumns(new Range(11))
            .names("name", "age", "year")
            .build();
    }

}
