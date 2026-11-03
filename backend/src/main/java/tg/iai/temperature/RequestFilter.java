package tg.iai.temperature;
import java.io.IOException;
import java.util.UUID;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
@Component
class RequestFilter extends OncePerRequestFilter {
 protected void doFilterInternal(HttpServletRequest r,HttpServletResponse s,FilterChain chain)throws ServletException,IOException{String id=r.getHeader("X-Request-ID");if(id==null||!id.matches("[a-zA-Z0-9-]{1,64}"))id=UUID.randomUUID().toString();s.setHeader("X-Request-ID",id);org.slf4j.MDC.put("requestId",id);try{if(r.getContentLengthLong()>16384){s.sendError(413);return;}chain.doFilter(r,s);}finally{org.slf4j.MDC.remove("requestId");}}
}
