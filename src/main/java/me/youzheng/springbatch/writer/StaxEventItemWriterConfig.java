package me.youzheng.springbatch.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;

@Profile("staxEventItemWriter")
@RequiredArgsConstructor
@Configuration
public class StaxEventItemWriterConfig {

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
            .<Object, Customer>chunk(3)
            .writer(xmlWriter())
            .reader(xmlReader())
            .build()
            ;
    }

    @Bean
    public ItemWriter<? super Customer> xmlWriter() {
        return new StaxEventItemWriterBuilder<Customer>()
            .name("xmlWriter")
            .marshaller(xStreamMarshaller())
            .resource(new FileSystemResource(
                "/Users/youzheng/workspace/study/batch/springbatch/src/main/resources/dist/customer.xml"))
            .rootTagName("customer")
            .build();
    }

    @Bean
    public ItemReader<Customer> xmlReader() {
        return new StaxEventItemReaderBuilder<Customer>()
            .name("staxEventReader")
            .resource(new ClassPathResource("/customer.xml"))
            .addFragmentRootElements("customer")
            .unmarshaller(xStreamMarshaller())
            .build()
            ;
    }

    @Bean
    public XStreamMarshaller xStreamMarshaller() {
        Map<String, Class<?>> aliases = new HashMap<>();
        aliases.put("customer", Customer.class);
        aliases.put("year", Integer.class);
        aliases.put("name", String.class);
        aliases.put("age", Integer.class);

        XStreamMarshaller marshaller = new XStreamMarshaller();
        marshaller.setAliases(aliases);
        return marshaller;
    }

}
