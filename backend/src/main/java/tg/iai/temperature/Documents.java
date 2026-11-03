package tg.iai.temperature;

import static org.springframework.data.mongodb.core.query.Criteria.where;

import java.time.*;
import java.util.*;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.stereotype.Component;

@Component
class Documents {
  final MongoTemplate mongo;

  Documents(MongoTemplate m) {
    mongo = m;
  }

  Document get(String collection, String id) {
    var d = mongo.findById(id, Document.class, collection);
    Rules.require(d != null, 404, "Ressource introuvable");
    return d;
  }

  Document optional(String c, String id) {
    return mongo.findById(id, Document.class, c);
  }

  Document insert(String c, Document d) {
    return mongo.insert(d, c);
  }

  void set(String c, String id, String field, Object value) {
    mongo.updateFirst(Query.query(where("_id").is(id)), Update.update(field, value), c);
  }

  Map<String, Object> view(Document d) {
    var m = new LinkedHashMap<String, Object>(d);
    m.put("id", m.remove("_id"));
    if (m.containsKey("publicationState")) {
      m.put("eventId", m.get("id"));
      m.put("statusUrl", "/api/v1/ingestions/" + m.get("id"));
    }
    return m;
  }

  Map<String, Object> page(String c, Criteria filter, int page, int size, Sort sort) {
    Rules.require(page >= 0 && page <= 100000, 400, "Page invalide");
    Rules.size(size, 100);
    var q = Query.query(filter);
    long count = mongo.count(q, c);
    var items =
        mongo.find(q.with(sort).skip((long) page * size).limit(size), Document.class, c).stream()
            .map(this::view)
            .toList();
    return Map.of("items", items, "page", page, "taille", size, "total", count);
  }
}
