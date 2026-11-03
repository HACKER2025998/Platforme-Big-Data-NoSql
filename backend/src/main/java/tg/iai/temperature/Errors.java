package tg.iai.temperature;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.http.converter.HttpMessageNotReadableException;
@RestControllerAdvice
class Errors {
 @ExceptionHandler(Rules.Failure.class) ResponseEntity<ProblemDetail> business(Rules.Failure e){return problem(e.status,e.getMessage());}
 @ExceptionHandler({IllegalArgumentException.class,HttpMessageNotReadableException.class,MethodArgumentTypeMismatchException.class,org.springframework.web.bind.MissingServletRequestParameterException.class}) ResponseEntity<ProblemDetail> bad(Exception e){return problem(400,"Paramètres ou JSON invalides");}
 @ExceptionHandler(DuplicateKeyException.class) ResponseEntity<ProblemDetail> duplicate(Exception e){return problem(409,"Identifiant déjà utilisé");}
 @ExceptionHandler(Exception.class) ResponseEntity<ProblemDetail> unavailable(Exception e){org.slf4j.LoggerFactory.getLogger(Errors.class).error("request_failed type={}",e.getClass().getSimpleName());return problem(503,"Service temporairement indisponible");}
 private ResponseEntity<ProblemDetail> problem(int s,String m){var p=ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(s),m);p.setProperty("code","HTTP_"+s);return ResponseEntity.status(s).body(p);}
}
