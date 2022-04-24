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
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;

@Profile("flatFile")
@Configuration
@RequiredArgsConstructor
public class FlatFileItemReaderConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job job() {
        return this.jobBuilderFactory.get("batchJob")
            .incrementer(new RunIdIncrementer())
            .start(step1())
            .next(step2())
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
            .name("flatFileItemReader")
            .resource(new ClassPathResource("/customer.csv"))
            .fieldSetMapper(new BeanWrapperFieldSetMapper<Customer>())
            .targetType(Customer.class)
            .linesToSkip(1)
            .delimited().delimiter(",")
            .names(new String[]{"name", "age", "year"})
            .build();
    }


    @Bean
    public ItemReader<Customer> customFlatFileItemReader() {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setLineMapper(customerLineMapper());
        itemReader.setLinesToSkip(1);
        itemReader.setResource(new ClassPathResource("/customer.csv"));
        return itemReader;
    }

    @Bean
    public LineMapper<Customer> customerLineMapper() {
        return new DefaultLineMapper<>(new DelimitedLineTokenizer(","), fieldSetMapper());
    }

    @Bean
    public CustomerFieldSetMapper fieldSetMapper() {
        CustomerFieldSetMapper customerFieldSetMapper = new CustomerFieldSetMapper();
        return customerFieldSetMapper;
    }

    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("step2")
            .tasklet(new Tasklet() {
                @Override
                public RepeatStatus execute(StepContribution contribution,
                    ChunkContext chunkContext) throws Exception {
                    System.out.println(">>> step1");
                    return RepeatStatus.FINISHED;
                }
            }).build();
    }

}
