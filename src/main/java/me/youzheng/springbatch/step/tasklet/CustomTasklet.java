package me.youzheng.springbatch.step.tasklet;

import java.util.Optional;
import jdk.nashorn.internal.runtime.options.Option;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.relational.core.sql.In;
import org.springframework.data.util.Optionals;

public class CustomTasklet implements Tasklet {

    private final int stepSeq;

    public CustomTasklet(int stepSeq) {
        this.stepSeq = stepSeq;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        System.out.println("step" + this.stepSeq + " was executed!");
        ExecutionContext context = contribution.getStepExecution().getJobExecution()
            .getExecutionContext();
        context.put("count", getValueOrDefault((Integer) context.get("count"), 0) + 1);
        return RepeatStatus.FINISHED;
    }

    public static <T> T getValueOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }
}
