package com.ashutosh.scheduler.worker.handler.sample;

import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.worker.handler.JobExecutionResult;
import com.ashutosh.scheduler.worker.handler.JobHandler;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"dev", "test"})
public class SleepJobHandler implements JobHandler {

    @Override
    public String type() {
        return "SLEEP";
    }

    @Override
    public JobExecutionResult execute(JobEntity job) {
        long sleepMs = 50;
        if (job.getBody() != null) {
            try {
                sleepMs = Math.min(Long.parseLong(job.getBody().trim()), 5000);
            } catch (NumberFormatException ignored) {}
        }
        try {
            Thread.sleep(sleepMs);
            return JobExecutionResult.success("Slept for " + sleepMs + "ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return JobExecutionResult.retryableFailure("Sleep interrupted");
        }
    }
}
