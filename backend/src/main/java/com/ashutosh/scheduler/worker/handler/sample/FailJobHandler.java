package com.ashutosh.scheduler.worker.handler.sample;

import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.worker.handler.JobExecutionResult;
import com.ashutosh.scheduler.worker.handler.JobHandler;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"dev", "test"})
public class FailJobHandler implements JobHandler {

    @Override
    public String type() {
        return "FAIL";
    }

    @Override
    public JobExecutionResult execute(JobEntity job) {
        String errorMsg = (job.getBody() == null || job.getBody().isBlank())
            ? "Simulated failure for testing"
            : job.getBody().trim();
        return JobExecutionResult.retryableFailure(errorMsg);
    }
}
