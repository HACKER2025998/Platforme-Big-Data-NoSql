package tg.iai.temperature;
import java.time.Duration;
import java.util.*;
import org.springframework.stereotype.Component;
import org.springframework.boot.health.contributor.*;
import org.springframework.kafka.core.KafkaAdmin;
import org.apache.kafka.clients.admin.AdminClient;
import com.datastax.oss.driver.api.core.CqlSession;
@Component("platform")
class PlatformHealth implements HealthIndicator {
 final Documents db;final CqlSession cql;final KafkaAdmin kafka;
 PlatformHealth(Documents d,CqlSession c,KafkaAdmin k){db=d;cql=c;kafka=k;}
 public Health health(){try{db.mongo.executeCommand("{ping:1}");cql.execute("SELECT capteur_id FROM derniere_mesure LIMIT 1");try(var client=AdminClient.create(kafka.getConfigurationProperties())){var names=client.listTopics().names().get(3,java.util.concurrent.TimeUnit.SECONDS);if(!names.containsAll(List.of("mesures-temperature","mesures-temperature-dlq")))return Health.down().build();}return Health.up().build();}catch(Exception e){return Health.down().withDetail("reason","DEPENDENCY_UNAVAILABLE").build();}}
}
