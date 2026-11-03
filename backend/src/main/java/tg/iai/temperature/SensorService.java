package tg.iai.temperature;

import static org.springframework.data.mongodb.core.query.Criteria.where;

import java.time.*;
import java.util.*;
import org.bson.Document;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.stereotype.Service;

@Service
class SensorService {
  final Documents db;
  final Clock clock;

  SensorService(Documents db, Clock c) {
    this.db = db;
    clock = c;
  }

  Document get(String id) {
    return db.get("capteurs", Rules.id(id));
  }

  Map<String, Object> create(Map<String, Object> m) {
    Rules.fields(m, "id", "libelle", "salle", "seuilTemperature", "actif");
    String id = Rules.id((String) m.get("id"));
    var d =
        new Document("_id", id)
            .append("libelle", Rules.text(m.get("libelle")))
            .append("salle", Rules.text(m.get("salle")))
            .append(
                "seuilTemperature",
                Rules.number(m.getOrDefault("seuilTemperature", 40)).doubleValue())
            .append("actif", bool(m.getOrDefault("actif", true)))
            .append("version", 0L)
            .append("createdAt", clock.instant().toString())
            .append("updatedAt", clock.instant().toString());
    return db.view(db.insert("capteurs", d));
  }

  boolean bool(Object o) {
    Rules.require(o instanceof Boolean, 400, "Booléen requis");
    return (Boolean) o;
  }

  Map<String, Object> patch(String id, Map<String, Object> m) {
    get(id);
    Rules.fields(m, "libelle", "salle", "seuilTemperature", "actif", "version");
    Rules.require(
        m.size() > 1
            && m.get("version") instanceof Number
            && m.get("version").toString().matches("[0-9]{1,18}"),
        400,
        "Version et modification requises");
    var u = new Update().inc("version", 1).set("updatedAt", clock.instant().toString());
    for (var k : m.keySet()) {
      if (k.equals("version")) continue;
      Object v =
          switch (k) {
            case "actif" -> bool(m.get(k));
            case "seuilTemperature" -> Rules.number(m.get(k)).doubleValue();
            default -> Rules.text(m.get(k));
          };
      u.set(k, v);
    }
    var d =
        db.mongo.findAndModify(
            Query.query(
                where("_id").is(id).and("version").is(((Number) m.get("version")).longValue())),
            u,
            FindAndModifyOptions.options().returnNew(true),
            Document.class,
            "capteurs");
    Rules.require(d != null, 409, "Version obsolète, rechargez le capteur");
    return db.view(d);
  }
}
