package me.youzheng.springbatch.parallel;


import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.listener.StopWatchJobListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Async
@Configuration
@RequiredArgsConstructor
@Profile("parallel")
public class ParallelConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource dataSource;

    @Bean
    public Job job() {
        return this.jobBuilderFactory.get("batchJob")
            .incrementer(new RunIdIncrementer())
            .listener(new StopWatchJobListener())
            .start(flow1())
            .split(taskExecutor())
            .add(flow2(), flow3())
            .build().build();
    }

    @Bean
    public Flow flow3() {
        Step step3 = this.stepBuilderFactory.get("step3").tasklet(tasklet()).build();

        return new FlowBuilder<Flow>("flow3")
            .start(step3)
            .build();
    }

    @Bean
    public Flow flow2() {

        Step step2 = this.stepBuilderFactory.get("step2").tasklet(tasklet()).build();

        return new FlowBuilder<Flow>("flow2")
            .start(step2)
            .build();
    }

    @Bean
    public Flow flow1() {
        return new FlowBuilder<Flow>("flow1")
            .start(step())
            .build();
    }

    @Bean
    public Step step() {
        return this.stepBuilderFactory.get("step1")
            .tasklet(tasklet()).build();
    }

    @Bean
    public Tasklet tasklet() {
        return new Tasklet() {

//            private long sum = 0;
            private long sum = 0;

            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
                throws Exception {
                for (int i = 0; i < 100000; i++) {
//                    synchronized(this) {
                        sum++;
//                    }
                }
                System.out.println(String.format("%s has been executed on thread %s",
                    chunkContext.getStepContext().getStepName(), Thread.currentThread().getName()));
                System.out.println(sum);
                return RepeatStatus.FINISHED;
            }
        };
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(Runtime.getRuntime().availableProcessors());
        executor.setMaxPoolSize(8);     // 최대 스레드 갯수
        executor.setThreadNamePrefix("async-thread");
        executor.setQueueCapacity(32);
        executor.initialize();
        return executor;
    }

}
