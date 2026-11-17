package tg.iai.temperature;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.springframework.stereotype.Component;

@Component
class HistoryStore {
  final CqlSession session;
  final CursorCodec cursors;

  HistoryStore(CqlSession s, CursorCodec c) {
    session = s;
    cursors = c;
  }

  Map<String, Object> row(Row r, String sensor) {
    return Map.of(
        "capteurId",
        sensor,
        "date",
        r.getInstant("date_mesure").toString(),
        "eventId",
        r.getString("event_id"),
        "temperature",
        r.getBigDecimal("temperature"));
  }

  void write(Document e) {
    String sensor = e.getString("capteurId"), id = e.getString("eventId");
    Instant date = Instant.parse(e.getString("date"));
    var temperature = Rules.number(e.get("temperature"));
    session.execute(
        session
            .prepare(
                "INSERT INTO mesures_par_capteur_jour"
                    + " (capteur_id,jour,date_mesure,event_id,temperature) VALUES (?,?,?,?,?)")
            .bind(sensor, date.atZone(ZoneOffset.UTC).toLocalDate(), date, id, temperature));
    String key = Rules.sortKey(date, id);
    var inserted =
        session.execute(
            session
                .prepare(
                    "INSERT INTO derniere_mesure"
                        + " (capteur_id,sort_key,date_mesure,event_id,temperature) VALUES"
                        + " (?,?,?,?,?) IF NOT EXISTS")
                .bind(sensor, key, date, id, temperature));
    if (!inserted.wasApplied()) {
      var result =
          session.execute(
              session
                  .prepare(
                      "UPDATE derniere_mesure SET sort_key=?,date_mesure=?,event_id=?,temperature=?"
                          + " WHERE capteur_id=? IF sort_key < ?")
                  .bind(key, date, id, temperature, sensor, key));
    }
  }

  Map<String, Object> latest(String sensor) {
    var r =
        session
            .execute(
                session
                    .prepare(
                        "SELECT date_mesure,event_id,temperature FROM derniere_mesure WHERE"
                            + " capteur_id=?")
                    .bind(sensor))
            .one();
    return r == null ? null : row(r, sensor);
  }

  List<Map<String, Object>> read(String sensor, Instant du, Instant au, int limit, String last) {
    List<Map<String, Object>> out = new ArrayList<>();
    LocalDate first = du.atZone(ZoneOffset.UTC).toLocalDate();
    for (LocalDate day = au.minusMillis(1).atZone(ZoneOffset.UTC).toLocalDate();
        !day.isBefore(first);
        day = day.minusDays(1)) {
      String sql =
          "SELECT date_mesure,event_id,temperature FROM mesures_par_capteur_jour WHERE capteur_id=?"
              + " AND jour=? AND date_mesure>=? AND date_mesure<?";
      for (var r : session.execute(session.prepare(sql).bind(sensor, day, du, au))) {
        String key = Rules.sortKey(r.getInstant("date_mesure"), r.getString("event_id"));
        if (last != null && key.compareTo(last) >= 0) continue;
        out.add(row(r, sensor));
        if (out.size() >= limit) return out;
      }
    }
    return out;
  }

  Map<String, Object> history(String id, Instant du, Instant au, int limit, String cursor) {
    Rules.period(du, au);
    Rules.size(limit, 500);
    String filter = id + "/" + du + "/" + au + "/" + limit;
    String last = cursor == null ? null : cursors.decode(cursor, filter);
    var rows = read(id, du, au, limit + 1, last);
    var response = new LinkedHashMap<String, Object>();
    String next = null;
    if (rows.size() > limit) {
      rows = new ArrayList<>(rows.subList(0, limit));
      var end = rows.getLast();
      next =
          cursors.encode(
              filter,
              Rules.sortKey(Instant.parse((String) end.get("date")), (String) end.get("eventId")));
    }
    response.put("items", rows);
    response.put("nextCursor", next);
    return response;
  }

  Map<String, Object> stats(String id, Instant du, Instant au) {
    Rules.period(du, au);
    var rows = read(id, du, au, 10001, null);
    Rules.require(rows.size() <= 10000, 422, "Plus de 10000 mesures, réduisez la période");
    var result = new LinkedHashMap<String, Object>();
    result.put("count", rows.size());
    var values = rows.stream().map(x -> (java.math.BigDecimal) x.get("temperature")).toList();
    result.put("min", values.stream().min(java.math.BigDecimal::compareTo).orElse(null));
    result.put("max", values.stream().max(java.math.BigDecimal::compareTo).orElse(null));
    result.put(
        "moyenne",
        values.isEmpty()
            ? null
            : values.stream()
                .reduce(java.math.BigDecimal.ZERO, java.math.BigDecimal::add)
                .divide(
                    java.math.BigDecimal.valueOf(values.size()),
                    4,
                    java.math.RoundingMode.HALF_UP));
    return result;
  }
}
