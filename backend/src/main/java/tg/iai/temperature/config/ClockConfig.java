package tg.iai.temperature.config;

import java.time.Clock;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Fournit l'horloge injectable et active les propriétés typées {@link PlatformProperties}.
 * Les services lisent le temps via {@code Instant.now(clock)} (tronqué à la milliseconde
 * si nécessaire) et jamais via {@code Instant.now()}, pour pouvoir utiliser
 * {@code Clock.fixed(...)} dans les tests.
 */
@Configuration
@EnableConfigurationProperties(PlatformProperties.class)
public class ClockConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
