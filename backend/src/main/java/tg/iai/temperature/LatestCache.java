package tg.iai.temperature;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

@Component
class LatestCache {
  final StringRedisTemplate redis;
  final HistoryStore history;
  final ObjectMapper json = new ObjectMapper();

  LatestCache(StringRedisTemplate r, HistoryStore h) {
    redis = r;
    history = h;
  }

  void put(Map<String, Object> m) {
    try {
      String key = Rules.sortKey(Instant.parse((String) m.get("date")), (String) m.get("eventId"));
      String lua =
          "local old=redis.call('HGET',KEYS[1],'sortKey'); if not old or old<ARGV[1] then"
              + " redis.call('HSET',KEYS[1],'sortKey',ARGV[1],'value',ARGV[2]);"
              + " redis.call('EXPIRE',KEYS[1],86400); return 1 end; return 0";
      redis.execute(
          new DefaultRedisScript<>(lua, Long.class),
          List.of("derniere:" + m.get("capteurId")),
          key,
          json.writeValueAsString(m));
    } catch (Exception e) {
      org.slf4j.LoggerFactory.getLogger(LatestCache.class).warn("cache_degraded operation=write");
    }
  }

  Map<String, Object> get(String id) {
    try {
      Object v = redis.opsForHash().get("derniere:" + id, "value");
      if (v != null)
        return json.readValue(
            v.toString(),
            new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
    } catch (Exception e) {
      org.slf4j.LoggerFactory.getLogger(LatestCache.class).warn("cache_degraded operation=read");
    }
    var m = history.latest(id);
    if (m != null) put(m);
    return m;
  }
}
