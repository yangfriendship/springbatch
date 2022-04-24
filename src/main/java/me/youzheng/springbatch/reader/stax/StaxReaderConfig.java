package me.youzheng.springbatch.reader.stax;

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
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;

@Profile("xmlReader")
@Configuration
@RequiredArgsConstructor
public class StaxReaderConfig {

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
            .reader(xmlReader())
            .writer(new ItemWriter<Customer>() {
                @Override
                public void write(List<? extends Customer> items) throws Exception {
                    System.out.println("items = " + items);
                }
            })
            .build()
            ;
    }

    private ItemReader<Customer> xmlReader() {
        return new StaxEventItemReaderBuilder<Customer>()
            .name("staxEventReader")
            .resource(new ClassPathResource("/customer.xml"))
            .addFragmentRootElements("customer")
            .unmarshaller(xStreamMarshaller())
            .build()
            ;
    }

    private Unmarshaller xStreamMarshaller() {

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
