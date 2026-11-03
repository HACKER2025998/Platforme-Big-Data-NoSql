package tg.iai.temperature;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class IngestionServiceTest {
 Documents db;SensorService sensors;IngestionService service;
 final Clock clock=Clock.fixed(Instant.parse("2026-11-03T12:00:00Z"),ZoneOffset.UTC);
 final String id="780c4ede-c522-4bc3-8d9c-9f100ed7a001";
 Map<String,Object> measure(){return Map.of("eventId",id,"capteurId","SALLE-01","date","2026-11-03T11:00:00Z","temperature",45.20);}
 @BeforeEach void setup(){db=mock(Documents.class);sensors=mock(SensorService.class);service=new IngestionService(db,sensors,clock);}
 @Test void capturesThresholdBeforeDurableReceipt(){when(sensors.get("SALLE-01")).thenReturn(new Document("actif",true).append("seuilTemperature",40.0));when(db.insert(eq("ingestions"),any())).thenAnswer(i->i.getArgument(1));when(db.view(any())).thenAnswer(i->new HashMap<>((Document)i.getArgument(0)));var r=service.submit(measure());assertEquals("PENDING",r.get("publicationState"));assertEquals(40.0,((Document)r.get("event")).get("seuilApplique"));verify(db).insert(eq("ingestions"),any());}
 @Test void replayDoesNotReadCurrentSensor(){var receipt=new Document("_id",id).append("empreinte","SALLE-01|2026-11-03T11:00:00Z|45.2");when(db.optional("ingestions",id)).thenReturn(receipt);when(db.view(receipt)).thenReturn(Map.of("id",id));assertEquals(id,service.submit(measure()).get("id"));verifyNoInteractions(sensors);}
 @Test void conflictingReplayNeverWrites(){when(db.optional("ingestions",id)).thenReturn(new Document("empreinte","different"));assertThrows(Rules.Failure.class,()->service.submit(measure()));verify(db,never()).insert(anyString(),any());verifyNoInteractions(sensors);}
 @Test void inactiveSensorNeverAdmitted(){when(sensors.get("SALLE-01")).thenReturn(new Document("actif",false));assertThrows(Rules.Failure.class,()->service.submit(measure()));verify(db,never()).insert(anyString(),any());}
 @Test void failureToWriteNeverReturnsReceipt(){when(sensors.get("SALLE-01")).thenReturn(new Document("actif",true).append("seuilTemperature",40));when(db.insert(anyString(),any())).thenThrow(new org.springframework.dao.DataAccessResourceFailureException("unavailable"));assertThrows(org.springframework.dao.DataAccessResourceFailureException.class,()->service.submit(measure()));verify(db,never()).view(any());}
}
