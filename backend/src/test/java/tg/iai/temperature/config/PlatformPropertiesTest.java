package tg.iai.temperature.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class PlatformPropertiesTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner().withUserConfiguration(ClockConfig.class);

    @Test
    void valeursParDefautConformesAuxReglesMetier() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            PlatformProperties p = context.getBean(PlatformProperties.class);

            assertThat(p.measurement().minTemperature()).isEqualTo(-50.0);
            assertThat(p.measurement().maxTemperature()).isEqualTo(100.0);
            assertThat(p.measurement().maxFutureSkew()).isEqualTo(Duration.ofMinutes(5));
            assertThat(p.measurement().minDate()).isEqualTo(Instant.parse("2000-01-01T00:00:00Z"));

            assertThat(p.query().defaultPageSize()).isEqualTo(20);
            assertThat(p.query().maxPageSize()).isEqualTo(100);
            assertThat(p.query().historyDefaultLimit()).isEqualTo(100);
            assertThat(p.query().historyMaxLimit()).isEqualTo(500);
            assertThat(p.query().statsMaxMeasures()).isEqualTo(10000);
            assertThat(p.query().maxPeriod()).isEqualTo(Duration.ofDays(7));

            assertThat(p.kafka().topic()).isEqualTo("mesures-temperature");
            assertThat(p.kafka().dlqTopic()).isEqualTo("mesures-temperature-dlq");
            assertThat(p.kafka().consumerGroup()).isEqualTo("traitement-temperature");

            assertThat(p.consumer().retryAttempts()).isEqualTo(3);
            assertThat(p.consumer().retryDelay()).isEqualTo(Duration.ofSeconds(2));
            assertThat(p.cache().latestTtl()).isEqualTo(Duration.ofHours(24));
        });
    }

    @Test
    void uneValeurValideEstPriseEnCompte() {
        runner.withPropertyValues("platform.cache.latest-ttl=PT1H", "platform.relay.batch-size=10")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    PlatformProperties p = context.getBean(PlatformProperties.class);
                    assertThat(p.cache().latestTtl()).isEqualTo(Duration.ofHours(1));
                    assertThat(p.relay().batchSize()).isEqualTo(10);
                });
    }

    @Test
    void bornesDeTemperatureInversees() {
        assertRejected("platform.measurement.min-temperature=100",
                "platform.measurement.max-temperature=100");
    }

    @Test
    void decalageFuturNul() {
        assertRejected("platform.measurement.max-future-skew=PT0S");
    }

    @Test
    void nombreDeTentativesNegatif() {
        assertRejected("platform.consumer.retry-attempts=-1");
    }

    @Test
    void tailleDeLotNulle() {
        assertRejected("platform.relay.batch-size=0");
    }

    @Test
    void backoffInitialSuperieurAuMaximum() {
        assertRejected("platform.relay.initial-backoff=PT5M", "platform.relay.max-backoff=PT1M");
    }

    @Test
    void topicVide() {
        assertRejected("platform.kafka.topic=");
    }

    @Test
    void topicEtDlqIdentiques() {
        assertRejected("platform.kafka.dlq-topic=mesures-temperature");
    }

    @Test
    void tailleMaximaleDeCorpsTropPetite() {
        assertRejected("platform.http.max-body-bytes=10");
    }

    @Test
    void taillePageParDefautSuperieureAuMaximum() {
        assertRejected("platform.query.default-page-size=200");
    }

    @Test
    void horlogeParDefautEstEnUtc() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(Clock.class).getZone()).isEqualTo(ZoneOffset.UTC);
        });
    }

    @Test
    void horlogeFixeUtilisableDansLesTests() {
        Clock fixed = Clock.fixed(Instant.parse("2026-10-28T07:00:00Z"), ZoneOffset.UTC);
        assertThat(Instant.now(fixed)).isEqualTo(Instant.parse("2026-10-28T07:00:00Z"));
    }

    private void assertRejected(String... properties) {
        runner.withPropertyValues(properties).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class);
        });
    }
}
