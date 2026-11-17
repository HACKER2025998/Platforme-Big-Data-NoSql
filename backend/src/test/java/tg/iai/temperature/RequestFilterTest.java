package tg.iai.temperature;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.*;

class RequestFilterTest {
  @Test
  void rejectsOversizedBody() {
    var req = new MockHttpServletRequest("POST", "/api/v1/mesures");
    req.setContent(new byte[16385]);
    var res = new MockHttpServletResponse();
    assertDoesNotThrow(
        () -> new RequestFilter().doFilter(req, res, (r, s) -> fail("Body must be refused")));
    assertEquals(413, res.getStatus());
    assertNotNull(res.getHeader("X-Request-ID"));
  }

  @Test
  void leavesReadableBodyForController() {
    var req = new MockHttpServletRequest("POST", "/api/v1/mesures");
    req.setContent("{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    var res = new MockHttpServletResponse();
    assertDoesNotThrow(
        () ->
            new RequestFilter()
                .doFilter(
                    req,
                    res,
                    (r, s) ->
                        assertEquals(
                            "{}",
                            new String(
                                r.getInputStream().readAllBytes(),
                                java.nio.charset.StandardCharsets.UTF_8))));
  }
}
