package me.youzheng.springbatch.chunk;

import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("basicChunk")
@Configuration
@RequiredArgsConstructor
public class BasicChunkConfig {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job job() {
        return this.jobBuilderFactory.get("batchJob")
            .start(step1())
            .incrementer(new RunIdIncrementer())
            .build();
    }

    private Step step1() {
        ArrayList<String> strings = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            strings.add("item" + i);
        }
        return this.stepBuilderFactory.get("step1")
            .<String, String>chunk(10)
            .reader(new ListItemReader<>(strings))
            .processor(new ItemProcessor<String, String>() {
                @Override
                public String process(String item) throws Exception {
                    return item.replace("item", "");
                }
            })
            /**
             * writer 은 개별이 아닌 일괄 처리
             * */
            .writer(new ItemWriter<String>() {
                @Override
                public void write(List<? extends String> items) throws Exception {
                    Thread.sleep(100);
                    System.out.println("items.size() = " + items.size());
                    System.out.println("items = " + items);
                }
            })
            /**
             * ChunkListener : Chunk 단위로 실행되는 이벤트
             * */
            .listener(new ChunkListener() {
                @Override
                public void beforeChunk(ChunkContext context) {
                    System.out.println("Before Chunk Step");
                }

                @Override
                public void afterChunk(ChunkContext context) {
                }

                @Override
                public void afterChunkError(ChunkContext context) {

                }
            })
            .listener(new StepExecutionListener() {
                @Override
                public void beforeStep(StepExecution stepExecution) {
                    System.out.println("----------------Before Step----------------");
                }

                @Override
                public ExitStatus afterStep(StepExecution stepExecution) {
                    System.out.println("----------------After Step----------------");
                    return stepExecution.getExitStatus();
                }
            })
            .build();
    }

}

