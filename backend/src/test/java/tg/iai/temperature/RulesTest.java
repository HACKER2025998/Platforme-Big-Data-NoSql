package tg.iai.temperature;
import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
class RulesTest {
 @Test void boundaries(){assertEquals("SALLE-01",Rules.id("SALLE-01"));for(String id:new String[]{"salle","","A B"})assertThrows(Rules.Failure.class,()->Rules.id(id));assertEquals(0,Rules.number("45.20").compareTo(Rules.number("45.2")));assertThrows(Rules.Failure.class,()->Rules.number("NaN"));assertThrows(Rules.Failure.class,()->Rules.number(101));}
 @Test void timeAndOrder(){var c=Clock.fixed(Instant.parse("2026-11-03T10:00:00Z"),ZoneOffset.UTC);Rules.dateRange(c.instant(),c);assertThrows(Rules.Failure.class,()->Rules.dateRange(c.instant().plusSeconds(301),c));assertThrows(Rules.Failure.class,()->Rules.period(c.instant(),c.instant().plusSeconds(604801)));assertTrue(Rules.sortKey(c.instant(),"a").compareTo(Rules.sortKey(c.instant(),"b"))<0);}
 @Test void cursorsBoundToFiltersAndSignature(){var codec=new CursorCodec("01234567890123456789012345678901");var token=codec.encode("sensor/period/100","last");assertEquals("last",codec.decode(token,"sensor/period/100"));assertThrows(Rules.Failure.class,()->codec.decode(token,"other"));assertThrows(Rules.Failure.class,()->codec.decode(token+"x","sensor/period/100"));}
 @Test void unknownFieldsRejected(){assertThrows(Rules.Failure.class,()->Rules.fields(Map.of("password","x"),"id"));}
}
