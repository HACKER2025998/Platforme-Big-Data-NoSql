package tg.iai.temperature;
import java.math.BigDecimal;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.springframework.http.HttpStatus;
public final class Rules {
 private Rules() {}
 public static class Failure extends RuntimeException { final int status; public Failure(int s,String m){super(m);status=s;} }
 public static void require(boolean ok,int status,String message){if(!ok)throw new Failure(status,message);}
 public static String id(String s){require(s!=null&&s.matches("[A-Z0-9_-]{1,64}"),400,"Identifiant capteur invalide");return s;}
 public static String uuid(String s){try {var u=UUID.fromString(s);require(u.toString().equalsIgnoreCase(s),400,"UUID invalide");return u.toString();}catch(Exception e){throw new Failure(400,"UUID invalide");}}
 public static String text(Object o){require(o instanceof String && !((String)o).isBlank()&&((String)o).length()<=120,400,"Texte requis, maximum 120 caractères");return ((String)o).strip();}
 public static BigDecimal number(Object o){try{var n=new BigDecimal(o.toString()).stripTrailingZeros();require(n.compareTo(BigDecimal.valueOf(-50))>=0&&n.compareTo(BigDecimal.valueOf(100))<=0,400,"Température hors plage [-50,100]");return n;}catch(Exception e){throw new Failure(400,"Température invalide, plage [-50,100]");}}
 public static Instant date(String s){try{return Instant.parse(s).truncatedTo(ChronoUnit.MILLIS);}catch(Exception e){throw new Failure(400,"Date UTC invalide");}}
 public static void dateRange(Instant d,Clock c){require(!d.isBefore(Instant.parse("2000-01-01T00:00:00Z"))&&!d.isAfter(c.instant().plusSeconds(300)),400,"Date hors limites (2000 à maintenant + 5 minutes)");}
 public static void period(Instant a,Instant b){require(a.isBefore(b)&&Duration.between(a,b).compareTo(Duration.ofDays(7))<=0,400,"Période requise, croissante et limitée à sept jours");}
 public static String sortKey(Instant d,String event){return String.format(Locale.ROOT,"%016d:%s",d.toEpochMilli(),event);}
 public static void fields(Map<String,Object> m,String... allowed){require(Arrays.asList(allowed).containsAll(m.keySet()),400,"Champ inconnu");}
 public static int size(int n,int max){require(n>0&&n<=max,400,"Taille hors limites");return n;}
}
