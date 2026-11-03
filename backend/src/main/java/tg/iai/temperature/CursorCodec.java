package tg.iai.temperature;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
@Component
class CursorCodec {
 final byte[] secret;
 CursorCodec(@Value("${platform.cursor-secret}") String s){Rules.require(s.length()>=32,500,"CURSOR_SECRET doit contenir au moins 32 caractères");secret=s.getBytes(StandardCharsets.UTF_8);}
 String sign(String s){try{var m=Mac.getInstance("HmacSHA256");m.init(new SecretKeySpec(secret,"HmacSHA256"));return Base64.getUrlEncoder().withoutPadding().encodeToString(m.doFinal(s.getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException(e);}}
 String encode(String filter,String last){String p=Base64.getUrlEncoder().withoutPadding().encodeToString((filter+"|"+last).getBytes(StandardCharsets.UTF_8));return p+"."+sign(p);}
 String decode(String token,String filter){try{Rules.require(token.length()<2048,400,"Curseur invalide");var parts=token.split("\\.");Rules.require(parts.length==2&&MessageDigest.isEqual(sign(parts[0]).getBytes(StandardCharsets.UTF_8),parts[1].getBytes(StandardCharsets.UTF_8)),400,"Signature de curseur invalide");String decoded=new String(Base64.getUrlDecoder().decode(parts[0]),StandardCharsets.UTF_8);Rules.require(decoded.startsWith(filter+"|"),400,"Curseur incompatible avec la recherche");return decoded.substring(filter.length()+1);}catch(Exception e){throw new Rules.Failure(400,"Curseur invalide ou incompatible");}}
}
