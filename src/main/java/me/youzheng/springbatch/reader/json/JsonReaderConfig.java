package me.youzheng.springbatch.reader.json;

import java.util.List;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.reader.flatfile.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;

@Profile("jsonReader")
@Configuration
@RequiredArgsConstructor
public class JsonReaderConfig {

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
            .reader(jsonReader())
            .writer(new ItemWriter<Customer>() {
                @Override
                public void write(List<? extends Customer> items) throws Exception {
                    System.out.println("items = " + items);
                }
            })
            .build()
            ;
    }

    @Bean
    public ItemReader<Customer> jsonReader() {
        return new JsonItemReaderBuilder<Customer>()
            .name("staxEventReader")
            .jsonObjectReader(new JacksonJsonObjectReader<>(Customer.class))
            .resource(new ClassPathResource("/customer.json"))
            .build()
            ;
    }

}
