package com.ashutosh.scheduler.worker.handler.sample;

import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.worker.handler.JobExecutionResult;
import com.ashutosh.scheduler.worker.handler.JobHandler;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"dev", "test"})
public class EchoJobHandler implements JobHandler {

    @Override
    public String type() {
        return "ECHO";
    }

    @Override
    public JobExecutionResult execute(JobEntity job) {
        String output = "Echo: " + (job.getBody() == null ? "" : job.getBody());
        return JobExecutionResult.success(output);
    }
}
