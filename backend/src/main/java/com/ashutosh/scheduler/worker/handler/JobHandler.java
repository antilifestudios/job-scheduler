package com.ashutosh.scheduler.worker.handler;

import com.ashutosh.scheduler.domain.entity.JobEntity;

/**
 * Strategy interface for executing jobs of a specific type.
 *
 * NOTE: Handlers execute strictly OUTSIDE any database transaction and
 * MUST NEVER hold database connections while executing.
 */
public interface JobHandler {

    /**
     * Unique job type string handled by this implementation (e.g. "HTTP", "ECHO").
     */
    String type();

    /**
     * Executes the job and returns an explicit outcome.
     *
     * @param job the job entity snapshot claimed by the worker
     * @return JobExecutionResult outcome
     */
    JobExecutionResult execute(JobEntity job);
}
