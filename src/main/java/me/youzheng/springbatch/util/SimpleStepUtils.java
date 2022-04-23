package me.youzheng.springbatch.util;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;

public class SimpleStepUtils {

    public static TaskletStepBuilder createPrintStep(StepBuilderFactory stepBuilderFactory, RepeatStatus status,
        ExitStatus exitStatus) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        String methodName = stackTrace[3].getMethodName();
        return stepBuilderFactory.get(methodName)
            .tasklet((contribution, chunkContext) -> {
                if (exitStatus != null) {
                    contribution.getStepExecution().setExitStatus(exitStatus);
                }
                System.out.println(">>> " + methodName + " is executed");
                return status;
            });
    }

    public static TaskletStepBuilder createPrintStep(StepBuilderFactory stepBuilderFactory) {
        return createPrintStep(stepBuilderFactory, RepeatStatus.FINISHED, null);
    }

    public static TaskletStepBuilder createPrintStep(StepBuilderFactory stepBuilderFactory,
        ExitStatus exitStatus) {
        return createPrintStep(stepBuilderFactory, RepeatStatus.FINISHED, exitStatus);
    }
}