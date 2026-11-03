package tg.iai.temperature;

import static org.springframework.data.mongodb.core.query.Criteria.where;

import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

@Component
class Pipeline {
  final Documents db;
  final HistoryStore history;
  final LatestCache cache;
  final KafkaTemplate<String, String> kafka;
  final Clock clock;
  final String topic, dlq;

  Pipeline(
      Documents d,
      HistoryStore h,
      LatestCache c,
      KafkaTemplate<String, String> k,
      Clock clock,
      @Value("${platform.topic}") String t,
      @Value("${platform.dlq}") String q) {
    db = d;
    history = h;
    cache = c;
    kafka = k;
    this.clock = clock;
    topic = t;
    dlq = q;
  }

  @Scheduled(fixedDelay = 2000)
  void relay() {
    try {
      var docs =
          db.mongo.find(
              Query.query(
                      where("publicationState")
                          .is("PENDING")
                          .and("nextAttemptAt")
                          .lte(clock.millis()))
                  .with(Sort.by("_id"))
                  .limit(50),
              Document.class,
              "ingestions");
      for (var d : docs) {
        String id = d.getString("_id");
        try {
          var e = d.get("event", Document.class);
          kafka.send(topic, e.getString("capteurId"), e.toJson()).get(10, TimeUnit.SECONDS);
          db.mongo.updateFirst(
              Query.query(where("_id").is(id)),
              new Update()
                  .set("publicationState", "PUBLISHED")
                  .set("publishedAt", clock.instant().toString())
                  .unset("lastErrorCode"),
              "ingestions");
        } catch (Exception ex) {
          int n = ((Number) d.get("tentatives")).intValue() + 1;
          db.mongo.updateFirst(
              Query.query(where("_id").is(id)),
              new Update()
                  .inc("tentatives", 1)
                  .set("nextAttemptAt", clock.millis() + Math.min(60000, 2000L * n))
                  .set("lastErrorCode", "PUBLICATION_UNAVAILABLE"),
              "ingestions");
        }
      }
    } catch (Exception ex) {
      org.slf4j.LoggerFactory.getLogger(Pipeline.class).warn("outbox_unavailable");
    }
  }

  @KafkaListener(topics = "${platform.topic}")
  void consume(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
    var e = Document.parse(record.value());
    String id = Rules.uuid(e.getString("eventId"));
    var receipt = db.get("ingestions", id);
    Rules.require(
        e.equals(receipt.get("event", Document.class))
            && Objects.equals(record.key(), e.getString("capteurId"))
            && e.getInteger("schemaVersion") == 1,
        400,
        "Événement non conforme au reçu");
    history.write(e);
    if (Rules.number(e.get("temperature")).compareTo(Rules.number(e.get("seuilApplique"))) > 0) {
      var u = new Update();
      u.setOnInsert("capteurId", e.get("capteurId"))
          .setOnInsert("date", e.get("date"))
          .setOnInsert("temperature", e.get("temperature"))
          .setOnInsert("seuilApplique", e.get("seuilApplique"))
          .setOnInsert("statut", "OPEN");
      db.mongo.upsert(Query.query(where("_id").is(id)), u, "alertes");
    }
    cache.put(
        Map.of(
            "capteurId",
            e.get("capteurId"),
            "date",
            e.get("date"),
            "temperature",
            e.get("temperature"),
            "eventId",
            id));
    db.mongo.updateFirst(
        Query.query(where("_id").is(id)),
        new Update()
            .set("processingState", "PROCESSED")
            .set("processedAt", clock.instant().toString())
            .unset("lastErrorCode"),
        "ingestions");
    org.slf4j.LoggerFactory.getLogger(Pipeline.class)
        .info("eventId={} phase=processing result=PROCESSED", id);
  }

  // Docker retire l'entrée DNS du courtier arrêté. Différer le démarrage permet
  // à l'API et à l'outbox de rester disponibles même au redémarrage de Java.
  @org.springframework.beans.factory.annotation.Autowired
  org.springframework.kafka.config.KafkaListenerEndpointRegistry listeners;

  @org.springframework.beans.factory.annotation.Autowired PlatformHealth readiness;

  @Scheduled(fixedDelay = 5000, initialDelay = 1000)
  void startConsumersWhenAvailable() {
    for (var container : listeners.getListenerContainers()) {
      if (!container.isRunning()) {
        if (!readiness.health().getStatus().getCode().equals("UP")) return;
        try {
          container.start();
        } catch (Exception unavailable) {
          // Le conteneur parent peut avoir commencé son démarrage avant l'échec du client.
          container.stop();
          org.slf4j.LoggerFactory.getLogger(Pipeline.class).warn("consumer_start_pending");
        }
      }
    }
  }

  @Bean
  DefaultErrorHandler kafkaErrorHandler() {
    var handler =
        new DefaultErrorHandler(
            (record, exception) -> {
              try {
                var dead =
                    new org.apache.kafka.clients.producer.ProducerRecord<String, String>(
                        dlq, (String) record.key(), (String) record.value());
                dead.headers()
                    .add(
                        "original-topic",
                        record.topic().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                dead.headers()
                    .add(
                        "original-partition",
                        Integer.toString(record.partition())
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                dead.headers()
                    .add(
                        "original-offset",
                        Long.toString(record.offset())
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                dead.headers()
                    .add(
                        "error-type",
                        exception
                            .getClass()
                            .getSimpleName()
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                kafka.send(dead).get(10, TimeUnit.SECONDS);
                try {
                  String id = Document.parse((String) record.value()).getString("eventId");
                  db.mongo.updateFirst(
                      Query.query(where("_id").is(id).and("processingState").ne("PROCESSED")),
                      new Update()
                          .set("processingState", "FAILED")
                          .set("lastErrorCode", "PROCESSING_FAILED"),
                      "ingestions");
                } catch (org.bson.json.JsonParseException malformed) {
                  org.slf4j.LoggerFactory.getLogger(Pipeline.class)
                      .error(
                          "dlq malformed topic={} partition={} offset={}",
                          record.topic(),
                          record.partition(),
                          record.offset());
                }
              } catch (Exception ex) {
                throw new IllegalStateException("DLQ recovery unavailable", ex);
              }
            },
            new FixedBackOff(2000L, 3));
    return handler;
  }
}
