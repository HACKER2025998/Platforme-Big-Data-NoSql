package tg.iai.temperature;
import java.net.URI;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.data.domain.Sort;
import static org.springframework.data.mongodb.core.query.Criteria.where;
@RestController @RequestMapping("/api/v1")
class ApiController {
 final Documents db;final SensorService sensors;final IngestionService ingestion;final HistoryStore history;final LatestCache latest;final Clock clock;
 ApiController(Documents d,SensorService s,IngestionService i,HistoryStore h,LatestCache l,Clock c){db=d;sensors=s;ingestion=i;history=h;latest=l;clock=c;}
 @GetMapping("/capteurs") Map<String,Object> sensors(@RequestParam(defaultValue="0")int page,@RequestParam(defaultValue="20")int taille,@RequestParam(required=false)String salle,@RequestParam(required=false)Boolean actif){var c=new Criteria();if(salle!=null)c.and("salle").is(salle);if(actif!=null)c.and("actif").is(actif);return db.page("capteurs",c,page,taille,Sort.by("_id"));}
 @PostMapping("/capteurs") ResponseEntity<?> create(@RequestBody Map<String,Object> m){var s=sensors.create(m);return ResponseEntity.created(URI.create("/api/v1/capteurs/"+s.get("id"))).body(s);}
 @GetMapping("/capteurs/{id}") Map<String,Object> sensor(@PathVariable String id){return db.view(sensors.get(id));}
 @PatchMapping("/capteurs/{id}") Map<String,Object> patch(@PathVariable String id,@RequestBody Map<String,Object> m){return sensors.patch(id,m);}
 @PostMapping("/mesures") ResponseEntity<?> submit(@RequestBody Map<String,Object> m){var r=ingestion.submit(m);return ResponseEntity.accepted().location(URI.create("/api/v1/ingestions/"+r.get("id"))).body(r);}
 @GetMapping("/ingestions/{eventId}") Map<String,Object> status(@PathVariable String eventId){return db.view(db.get("ingestions",Rules.uuid(eventId)));}
 @GetMapping("/capteurs/{id}/derniere-mesure") ResponseEntity<?> latest(@PathVariable String id){sensors.get(id);var m=latest.get(id);return m==null?ResponseEntity.noContent().build():ResponseEntity.ok(m);}
 @GetMapping("/capteurs/{id}/mesures") Map<String,Object> history(@PathVariable String id,@RequestParam String du,@RequestParam String au,@RequestParam(defaultValue="100")int limite,@RequestParam(required=false)String curseur){sensors.get(id);return history.history(id,Rules.date(du),Rules.date(au),limite,curseur);}
 @GetMapping("/capteurs/{id}/statistiques") Map<String,Object> stats(@PathVariable String id,@RequestParam String du,@RequestParam String au){sensors.get(id);return history.stats(id,Rules.date(du),Rules.date(au));}
 @GetMapping("/alertes") Map<String,Object> alerts(@RequestParam(defaultValue="0")int page,@RequestParam(defaultValue="20")int taille,@RequestParam(required=false)String capteurId,@RequestParam(required=false)String statut){var c=new Criteria();if(capteurId!=null)c.and("capteurId").is(Rules.id(capteurId));if(statut!=null){Rules.require(List.of("OPEN","ACKNOWLEDGED").contains(statut),400,"Statut inconnu");c.and("statut").is(statut);}return db.page("alertes",c,page,taille,Sort.by(Sort.Order.desc("date"),Sort.Order.desc("_id")));}
 @PatchMapping("/alertes/{id}/acquittement") Map<String,Object> acknowledge(@PathVariable String id){Rules.uuid(id);db.mongo.updateFirst(Query.query(where("_id").is(id).and("statut").is("OPEN")),new Update().set("statut","ACKNOWLEDGED").set("dateAcquittement",clock.instant().toString()),"alertes");return db.view(db.get("alertes",id));}
}
