package dev.mahir.ledger.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "ledger.outbox")
public class OutboxProperties {

    /** How often each drain worker polls, in ms. */
    private long drainFixedDelayMs = 100;
    /** Max events one worker claims per tick. */
    private int batchSize = 50;
    /** Attempts before an event is marked DEAD. */
    private int maxAttempts = 5;
    private long backoffBaseMs = 200;
    private long backoffCapMs = 30_000;
    /** "in-process" (default) or "webhook". */
    private String sink = "in-process";
    private String webhookUrl = "";
    /** Number of concurrent drain workers. */
    private int workerCount = 1;

    public long getDrainFixedDelayMs() {
        return drainFixedDelayMs;
    }

    public void setDrainFixedDelayMs(long drainFixedDelayMs) {
        this.drainFixedDelayMs = drainFixedDelayMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getBackoffBaseMs() {
        return backoffBaseMs;
    }

    public void setBackoffBaseMs(long backoffBaseMs) {
        this.backoffBaseMs = backoffBaseMs;
    }

    public long getBackoffCapMs() {
        return backoffCapMs;
    }

    public void setBackoffCapMs(long backoffCapMs) {
        this.backoffCapMs = backoffCapMs;
    }

    public String getSink() {
        return sink;
    }

    public void setSink(String sink) {
        this.sink = sink;
    }

    public String getWebhookUrl() {
        return webhookUrl;
    }

    public void setWebhookUrl(String webhookUrl) {
        this.webhookUrl = webhookUrl;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }
}
