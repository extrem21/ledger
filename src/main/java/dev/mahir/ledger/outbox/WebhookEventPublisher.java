package dev.mahir.ledger.outbox;

import dev.mahir.ledger.config.OutboxProperties;
import dev.mahir.ledger.store.OutboxEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

/**
 * Publishes by POSTing the event payload to a configured URL. Used to point
 * T5 (poison event) at an endpoint that always returns 500 — not the
 * production default.
 */
@Component
@ConditionalOnProperty(name = "ledger.outbox.sink", havingValue = "webhook")
public class WebhookEventPublisher implements EventPublisher {

    private final RestTemplate restTemplate;
    private final OutboxProperties properties;

    public WebhookEventPublisher(RestTemplateBuilder builder, OutboxProperties properties) {
        this.restTemplate = builder
                .setConnectTimeout(Duration.ofSeconds(2))
                .setReadTimeout(Duration.ofSeconds(5))
                .build();
        this.properties = properties;
    }

    @Override
    public void publish(OutboxEvent event) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(event.payload(), headers);
        try {
            restTemplate.postForEntity(properties.getWebhookUrl(), request, Void.class);
        } catch (RestClientException e) {
            throw new PublishException("webhook call to " + properties.getWebhookUrl() + " failed: " + e.getMessage(), e);
        }
    }
}
