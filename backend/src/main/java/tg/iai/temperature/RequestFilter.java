package tg.iai.temperature;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
class RequestFilter extends OncePerRequestFilter {
  protected void doFilterInternal(HttpServletRequest r, HttpServletResponse s, FilterChain chain)
      throws ServletException, IOException {
    String id = r.getHeader("X-Request-ID");
    if (id == null || !id.matches("[a-zA-Z0-9-]{1,64}")) id = UUID.randomUUID().toString();
    s.setHeader("X-Request-ID", id);
    org.slf4j.MDC.put("requestId", id);
    try {
      if (r.getMethod().equals("POST") || r.getMethod().equals("PATCH")) {
        byte[] bytes = r.getInputStream().readNBytes(16385);
        if (bytes.length > 16384) {
          s.setStatus(413);
          s.setContentType("application/problem+json");
          s.getWriter()
              .write(
                  "{\"status\":413,\"code\":\"HTTP_413\",\"detail\":\"Corps limité à 16384"
                      + " octets\"}");
          return;
        }
        r =
            new HttpServletRequestWrapper(r) {
              public ServletInputStream getInputStream() {
                var stream = new ByteArrayInputStream(bytes);
                return new ServletInputStream() {
                  public int read() {
                    return stream.read();
                  }

                  public boolean isFinished() {
                    return stream.available() == 0;
                  }

                  public boolean isReady() {
                    return true;
                  }

                  public void setReadListener(ReadListener listener) {
                    throw new UnsupportedOperationException("Synchronous requests only");
                  }
                };
              }

              public BufferedReader getReader() {
                return new BufferedReader(
                    new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
              }
            };
      }
      chain.doFilter(r, s);
    } finally {
      org.slf4j.MDC.remove("requestId");
    }
  }
}
