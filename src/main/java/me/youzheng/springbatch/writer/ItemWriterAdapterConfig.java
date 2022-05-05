package me.youzheng.springbatch.writer;

import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.reader.flatfile.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.adapter.ItemWriterAdapter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;

@Profile("itemWriterAdapter")
@Configuration
@RequiredArgsConstructor
public class ItemWriterAdapterConfig {

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
            .writer(jsonWriter())
            .build()
            ;
    }

    public static class CustomWriterService<T> {

        public void write(T item) {
            System.out.println(item + " in CustomWriterService");
        }

    }

    @Bean
    public ItemWriter<Customer> jsonWriter() {
        ItemWriterAdapter<Customer> adapter = new ItemWriterAdapter<>();
        adapter.setTargetObject(customWriterService());
        adapter.setTargetMethod("write");
        return adapter;
    }

    @Bean
    public CustomWriterService<Customer> customWriterService() {
        return new CustomWriterService<>();
    }

    @Bean
    public JsonObjectMarshaller<Customer> jsonMarshaller() {
        return new JacksonJsonObjectMarshaller<>();
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
