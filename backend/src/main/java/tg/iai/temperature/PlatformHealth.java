package tg.iai.temperature;

import com.datastax.oss.driver.api.core.CqlSession;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.health.contributor.*;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@Component("platform")
class PlatformHealth implements HealthIndicator {
  final Documents db;
  final CqlSession cql;
  final KafkaAdmin kafka;

  PlatformHealth(Documents d, CqlSession c, KafkaAdmin k) {
    db = d;
    cql = c;
    kafka = k;
  }

  public Health health() {
    try {
      db.mongo.executeCommand("{ping:1}");
      cql.execute("SELECT capteur_id FROM derniere_mesure LIMIT 1");
      var config = new HashMap<String, Object>(kafka.getConfigurationProperties());
      config.put("request.timeout.ms", 2000);
      config.put("default.api.timeout.ms", 3000);
      var client = AdminClient.create(config);
      try {
        var names = client.listTopics().names().get(3, java.util.concurrent.TimeUnit.SECONDS);
        if (!names.containsAll(List.of("mesures-temperature", "mesures-temperature-dlq")))
          return Health.down().build();
      } finally {
        client.close(Duration.ZERO);
      }
      return Health.up().build();
    } catch (Exception e) {
      return Health.down().withDetail("reason", "DEPENDENCY_UNAVAILABLE").build();
    }
  }
}
