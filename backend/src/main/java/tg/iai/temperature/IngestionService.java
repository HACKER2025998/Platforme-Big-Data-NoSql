package tg.iai.temperature;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.springframework.stereotype.Service;
import org.springframework.dao.DuplicateKeyException;
@Service
class IngestionService {
 final Documents db;final SensorService sensors;final Clock clock;
 IngestionService(Documents d,SensorService s,Clock c){db=d;sensors=s;clock=c;}
 Map<String,Object> submit(Map<String,Object> m){Rules.fields(m,"eventId","capteurId","date","temperature");String id=Rules.uuid((String)m.get("eventId")),sensor=Rules.id((String)m.get("capteurId"));var date=Rules.date((String)m.get("date"));Rules.dateRange(date,clock);var temperature=Rules.number(m.get("temperature"));String fingerprint=sensor+"|"+date+"|"+temperature.toPlainString();var old=db.optional("ingestions",id);if(old!=null)return duplicate(old,fingerprint);var s=sensors.get(sensor);Rules.require(s.getBoolean("actif"),409,"Capteur inactif");var event=new Document("schemaVersion",1).append("eventId",id).append("capteurId",sensor).append("date",date.toString()).append("temperature",temperature.doubleValue()).append("seuilApplique",s.get("seuilTemperature")).append("acceptedAt",clock.instant().toString());var receipt=new Document("_id",id).append("event",event).append("empreinte",fingerprint).append("publicationState","PENDING").append("processingState","PENDING").append("tentatives",0).append("nextAttemptAt",0L);try{return db.view(db.insert("ingestions",receipt));}catch(DuplicateKeyException e){return duplicate(db.get("ingestions",id),fingerprint);}}
 Map<String,Object> duplicate(Document d,String fingerprint){Rules.require(fingerprint.equals(d.getString("empreinte")),409,"eventId déjà associé à une autre mesure");return db.view(d);}
}
