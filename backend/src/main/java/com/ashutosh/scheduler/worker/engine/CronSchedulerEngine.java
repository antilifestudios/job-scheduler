package com.ashutosh.scheduler.worker.engine;

import com.ashutosh.scheduler.domain.entity.ScheduleEntity;
import com.ashutosh.scheduler.domain.repo.ScheduleRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class CronSchedulerEngine {

    private static final Logger log = LoggerFactory.getLogger(CronSchedulerEngine.class);
    private static final int BATCH_SIZE = 25;

    private final ScheduleRepository scheduleRepository;
    private final CronScheduleProcessor scheduleProcessor;
    private final boolean enabled;
    private final long timeBudgetMs;

    public CronSchedulerEngine(
            ScheduleRepository scheduleRepository,
            CronScheduleProcessor scheduleProcessor,
            @Value("${scheduler.cron.enabled:true}") boolean enabled,
            @Value("${scheduler.cron.time-budget-ms:2000}") long timeBudgetMs) {
        this.scheduleRepository = scheduleRepository;
        this.scheduleProcessor = scheduleProcessor;
        this.enabled = enabled;
        this.timeBudgetMs = timeBudgetMs;
    }

    @Scheduled(fixedDelayString = "${scheduler.cron.poll-interval-ms:1000}")
    public void scheduledPoll() {
        if (!enabled) {
            return;
        }
        pollOnce();
    }

    /**
     * Drains all due schedules within the allocated time budget.
     * Accessible for deterministic testing.
     *
     * @return total number of schedules evaluated in this tick
     */
    public int pollOnce() {
        long budgetNanos = TimeUnit.MILLISECONDS.toNanos(timeBudgetMs);
        long startNanos = System.nanoTime();
        int totalProcessed = 0;

        while (System.nanoTime() - startNanos < budgetNanos) {
            List<ScheduleEntity> dueSchedules = scheduleRepository.claimDueSchedules(BATCH_SIZE);
            if (dueSchedules.isEmpty()) {
                break;
            }

            for (ScheduleEntity schedule : dueSchedules) {
                if (System.nanoTime() - startNanos >= budgetNanos) {
                    log.warn("Cron sweeper exhausted time budget of {}ms after processing {} schedules; yielding",
                        timeBudgetMs, totalProcessed);
                    break;
                }

                try {
                    scheduleProcessor.processSchedule(schedule.getId(), Instant.now());
                    totalProcessed++;
                } catch (Exception e) {
                    log.error("Unexpected error delegating schedule {}: {}", schedule.getId(), e.getMessage(), e);
                }
            }
        }

        return totalProcessed;
    }
}
