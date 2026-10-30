package tg.iai.temperature.config;

import java.time.Duration;
import java.time.Instant;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Propriétés techniques du projet, préfixe {@code platform}.
 * Toutes ont une valeur par défaut conforme à docs/regles-metier.md ; une valeur invalide
 * fait échouer le démarrage avec un message de validation.
 *
 * <pre>
 * platform:
 *   measurement:
 *     min-temperature: -50
 *     max-temperature: 100
 *     max-future-skew: PT5M
 *     min-date: 2000-01-01T00:00:00Z
 *   query:
 *     default-page-size: 20
 *     max-page-size: 100
 *     history-default-limit: 100
 *     history-max-limit: 500
 *     stats-max-measures: 10000
 *     max-period: P7D
 *   kafka:
 *     topic: mesures-temperature
 *     dlq-topic: mesures-temperature-dlq
 *     consumer-group: traitement-temperature
 *   relay:
 *     batch-size: 50
 *     poll-interval: PT1S
 *     initial-backoff: PT1S
 *     max-backoff: PT1M
 *   consumer:
 *     retry-attempts: 3
 *     retry-delay: PT2S
 *   cache:
 *     latest-ttl: PT24H
 *   http:
 *     max-body-bytes: 16384
 * </pre>
 */
@ConfigurationProperties(prefix = "platform")
@Validated
public record PlatformProperties(
        @Valid @DefaultValue Measurement measurement,
        @Valid @DefaultValue Query query,
        @Valid @DefaultValue Kafka kafka,
        @Valid @DefaultValue Relay relay,
        @Valid @DefaultValue Consumer consumer,
        @Valid @DefaultValue Cache cache,
        @Valid @DefaultValue Http http) {

    /** Règles de validation d'une mesure (docs/regles-metier.md, sections 2 et 4). */
    public record Measurement(
            @DefaultValue("-50") double minTemperature,
            @DefaultValue("100") double maxTemperature,
            @NotNull @DurationMin(millis = 1) @DefaultValue("PT5M") Duration maxFutureSkew,
            @NotNull @DefaultValue("2000-01-01T00:00:00Z") Instant minDate) {

        @AssertTrue(message = "min-temperature doit être strictement inférieure à max-temperature")
        public boolean isTemperatureRangeValid() {
            return minTemperature < maxTemperature;
        }
    }

    /** Limites des lectures (pagination, historique, statistiques). */
    public record Query(
            @Min(1) @DefaultValue("20") int defaultPageSize,
            @Min(1) @DefaultValue("100") int maxPageSize,
            @Min(1) @DefaultValue("100") int historyDefaultLimit,
            @Min(1) @DefaultValue("500") int historyMaxLimit,
            @Min(1) @DefaultValue("10000") int statsMaxMeasures,
            @NotNull @DurationMin(millis = 1) @DefaultValue("P7D") Duration maxPeriod) {

        @AssertTrue(message = "default-page-size doit être inférieure ou égale à max-page-size")
        public boolean isPageSizesValid() {
            return defaultPageSize <= maxPageSize;
        }

        @AssertTrue(message = "history-default-limit doit être inférieure ou égale à history-max-limit")
        public boolean isHistoryLimitsValid() {
            return historyDefaultLimit <= historyMaxLimit;
        }
    }

    /** Topics et groupe de consommation Kafka. */
    public record Kafka(
            @NotBlank @DefaultValue("mesures-temperature") String topic,
            @NotBlank @DefaultValue("mesures-temperature-dlq") String dlqTopic,
            @NotBlank @DefaultValue("traitement-temperature") String consumerGroup) {

        @AssertTrue(message = "topic et dlq-topic doivent être différents")
        public boolean isTopicsDistinct() {
            return topic == null || !topic.equals(dlqTopic);
        }
    }

    /** Relais de publication de l'outbox. */
    public record Relay(
            @Min(1) @Max(1000) @DefaultValue("50") int batchSize,
            @NotNull @DurationMin(millis = 1) @DefaultValue("PT1S") Duration pollInterval,
            @NotNull @DurationMin(millis = 1) @DefaultValue("PT1S") Duration initialBackoff,
            @NotNull @DurationMin(millis = 1) @DefaultValue("PT1M") Duration maxBackoff) {

        @AssertTrue(message = "initial-backoff doit être inférieur ou égal à max-backoff")
        public boolean isBackoffValid() {
            return initialBackoff == null || maxBackoff == null
                    || initialBackoff.compareTo(maxBackoff) <= 0;
        }
    }

    /** Reprises du consommateur Kafka avant envoi en DLQ. */
    public record Consumer(
            @Min(0) @Max(10) @DefaultValue("3") int retryAttempts,
            @NotNull @DurationMin(millis = 1) @DefaultValue("PT2S") Duration retryDelay) {
    }

    /** Cache Redis de la dernière mesure. */
    public record Cache(
            @NotNull @DurationMin(seconds = 1) @DefaultValue("PT24H") Duration latestTtl) {
    }

    /** Limites HTTP (utilisées par T072). */
    public record Http(
            @Min(1024) @Max(1048576) @DefaultValue("16384") int maxBodyBytes) {
    }
}
