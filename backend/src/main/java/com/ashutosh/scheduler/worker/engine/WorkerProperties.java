package com.ashutosh.scheduler.worker.engine;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "scheduler.worker")
public class WorkerProperties {

    private boolean enabled = true;
    private int concurrency = 10;
    private int leaseDurationSeconds = 30;
    private long pollIntervalMs = 500;
    private long maxPollIntervalMs = 2000;
    private int batchSize = 10;
    private int baseBackoffSeconds = 5;
    private int maxBackoffSeconds = 3600;
    private boolean reaperEnabled = true;
    private long reaperIntervalMs = 10000;
    private int reaperBatchSize = 50;
    private int shutdownTimeoutSeconds = 15;
    private boolean ssrfEnabled = true;
    private boolean ssrfAllowPrivateIps = false;

    public boolean isSsrfEnabled() { return ssrfEnabled; }
    public void setSsrfEnabled(boolean ssrfEnabled) { this.ssrfEnabled = ssrfEnabled; }

    public boolean isSsrfAllowPrivateIps() { return ssrfAllowPrivateIps; }
    public void setSsrfAllowPrivateIps(boolean ssrfAllowPrivateIps) { this.ssrfAllowPrivateIps = ssrfAllowPrivateIps; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public int getConcurrency() { return concurrency; }
    public void setConcurrency(int concurrency) { this.concurrency = concurrency; }

    public int getLeaseDurationSeconds() { return leaseDurationSeconds; }
    public void setLeaseDurationSeconds(int leaseDurationSeconds) { this.leaseDurationSeconds = leaseDurationSeconds; }

    public long getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }

    public long getMaxPollIntervalMs() { return maxPollIntervalMs; }
    public void setMaxPollIntervalMs(long maxPollIntervalMs) { this.maxPollIntervalMs = maxPollIntervalMs; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public int getBaseBackoffSeconds() { return baseBackoffSeconds; }
    public void setBaseBackoffSeconds(int baseBackoffSeconds) { this.baseBackoffSeconds = baseBackoffSeconds; }

    public int getMaxBackoffSeconds() { return maxBackoffSeconds; }
    public void setMaxBackoffSeconds(int maxBackoffSeconds) { this.maxBackoffSeconds = maxBackoffSeconds; }

    public boolean isReaperEnabled() { return reaperEnabled; }
    public void setReaperEnabled(boolean reaperEnabled) { this.reaperEnabled = reaperEnabled; }

    public long getReaperIntervalMs() { return reaperIntervalMs; }
    public void setReaperIntervalMs(long reaperIntervalMs) { this.reaperIntervalMs = reaperIntervalMs; }

    public int getReaperBatchSize() { return reaperBatchSize; }
    public void setReaperBatchSize(int reaperBatchSize) { this.reaperBatchSize = reaperBatchSize; }

    public int getShutdownTimeoutSeconds() { return shutdownTimeoutSeconds; }
    public void setShutdownTimeoutSeconds(int shutdownTimeoutSeconds) { this.shutdownTimeoutSeconds = shutdownTimeoutSeconds; }
}
