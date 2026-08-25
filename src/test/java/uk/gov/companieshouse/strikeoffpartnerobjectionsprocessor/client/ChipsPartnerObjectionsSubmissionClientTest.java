package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

class ChipsPartnerObjectionsSubmissionClientTest {
    private static final String BASE_URL = "http://chips-rest-interfaces";
    private static final String ENDPOINT_URL = BASE_URL + "/chipsgeneric/strike-off-partner-objections";

    private MockRestServiceServer server;
    private ChipsPartnerObjectionsSubmissionClient client;

    @BeforeEach
    void setUp() {
        RestTemplate restTemplate = new RestTemplate();
        this.server = MockRestServiceServer.bindTo(restTemplate).build();
        this.client = new ChipsPartnerObjectionsSubmissionClient(restTemplate, BASE_URL);
    }

    @Test
    void submit_postsRequestToDedicatedEndpoint_andHandles202AsSuccess() {
        server.expect(requestTo(ENDPOINT_URL))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.eventId").value("evt-100"))
                .andExpect(jsonPath("$.eventType").value("OBJECTION"))
                .andExpect(jsonPath("$.companyNumber").value("12345678"))
                .andExpect(jsonPath("$.partnerOrganisation").value("TEST_ORG"))
                .andRespond(withStatus(HttpStatus.ACCEPTED));

        assertDoesNotThrow(() -> client.submit(buildMessage(EventType.OBJECTION)));
        server.verify();
    }

    @Test
    void submit_throwsNonRetryableFor403Response() {
        server.expect(requestTo(ENDPOINT_URL))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.FORBIDDEN));
        StrikeOffPartnerObjections message = buildMessage(EventType.WITHDRAWAL);

        ChipsSubmissionException exception = assertThrows(
                ChipsSubmissionException.class,
                () -> client.submit(message));

        assertEquals(403, exception.getStatusCode());
    }

    @Test
    void submit_throwsWhenStatusIsNot202() {
        server.expect(requestTo(ENDPOINT_URL))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));
        StrikeOffPartnerObjections message = buildMessage(EventType.OBJECTION);

        ChipsSubmissionException exception = assertThrows(
                ChipsSubmissionException.class,
                () -> client.submit(message));

        assertEquals(200, exception.getStatusCode());
    }

    private StrikeOffPartnerObjections buildMessage(EventType eventType) {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-100")
                .setEventTime("2026-08-24T00:00:00Z")
                .setSource("test")
                .setEventType(eventType)
                .setCompanyNumber("12345678")
                .setPartnerOrganisation("TEST_ORG")
                .setStrikeOffEventId("strike-100")
                .build();
    }
}
