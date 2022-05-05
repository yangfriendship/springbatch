package me.youzheng.springbatch.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class PassCheckingListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {

    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        String exitCode = stepExecution.getExitStatus().getExitCode();
        // StepExist 상태가 FAILED 가 아나리면 PASS 로 변경
        if (!exitCode.equals(ExitStatus.FAILED.getExitCode())) {
            return new ExitStatus("PASS");
        }
        return null;
    }
}
