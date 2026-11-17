package tg.iai.temperature;

import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Clock;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class InvalidJsonTypeTest {
  @Test
  void wrongJsonTypesReturnClientError() throws Exception {
    var db = mock(Documents.class);
    var clock = Clock.systemUTC();
    var sensors = new SensorService(db, clock);
    var api =
        new ApiController(db, sensors, new IngestionService(db, sensors, clock), null, null, clock);
    var mvc = MockMvcBuilders.standaloneSetup(api).setControllerAdvice(new Errors()).build();
    mvc.perform(
            post("/api/v1/capteurs")
                .contentType("application/json")
                .content("{\"id\":123,\"libelle\":\"Test\",\"salle\":\"Test\"}"))
        .andExpect(status().isBadRequest());
    mvc.perform(
            post("/api/v1/mesures")
                .contentType("application/json")
                .content(
                    "{\"eventId\":\"780c4ede-c522-4bc3-8d9c-9f100ed7a001\",\"capteurId\":123,\"date\":\"2026-11-03T10:00:00Z\",\"temperature\":25}"))
        .andExpect(status().isBadRequest());
  }
}
