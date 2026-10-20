package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

class MockChipsPartnerObjectionsSubmissionClientTest {

    private static final String CALLBACK_URL =
            "http://localhost/private/strike-off-partner-objections-processed";
    private static final String COMPANY_NUMBER = "12345678";
    private static final String STRIKE_OFF_EVENT_ID = "strike-100";
    private static final String API_KEY = "test-key";
    private static final String EXPECTED_AUTH = "Basic " + Base64.getEncoder().encodeToString((API_KEY + ":").getBytes(StandardCharsets.UTF_8));


    private MockRestServiceServer server;
    private MockChipsPartnerObjectionsSubmissionClient client;

    @BeforeEach
    void setUp() {
        RestTemplate restTemplate = new RestTemplate();
        server = MockRestServiceServer.bindTo(restTemplate).build();
        client = new MockChipsPartnerObjectionsSubmissionClient(
                restTemplate, CALLBACK_URL, API_KEY);
    }

    @Test
    void objectionPostsSuccessfulProcessedCallback() {
            server.expect(requestTo(CALLBACK_URL))
                .andExpect(method(HttpMethod.POST))
                .andExpect(header(HttpHeaders.AUTHORIZATION, EXPECTED_AUTH))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.strike_off_event_id").value(STRIKE_OFF_EVENT_ID))
                .andExpect(jsonPath("$.company_number").value(COMPANY_NUMBER))
                .andExpect(jsonPath("$.event_type").value("OBJECTION"))
                .andExpect(jsonPath("$.success_failure_indicator").value("SUCCESS"))
                .andExpect(jsonPath("$.initial_expiration_on").exists())
                .andRespond(withStatus(HttpStatus.ACCEPTED));

        client.submitForObjections(
                new BaseObjectionResponse().companyNumber(COMPANY_NUMBER),
                message(EventType.OBJECTION));

        server.verify();
    }

    @Test
    void withdrawalPostsSuccessfulProcessedCallbackWithoutExpirationDate() {
        server.expect(requestTo(CALLBACK_URL))
                .andExpect(method(HttpMethod.POST))
                .andExpect(header(HttpHeaders.AUTHORIZATION, EXPECTED_AUTH))
                .andExpect(jsonPath("$.strike_off_event_id").value(STRIKE_OFF_EVENT_ID))
                .andExpect(jsonPath("$.company_number").value(COMPANY_NUMBER))
                .andExpect(jsonPath("$.event_type").value("WITHDRAWAL"))
                .andExpect(jsonPath("$.success_failure_indicator").value("SUCCESS"))
                .andExpect(jsonPath("$.initial_expiration_on").doesNotExist())
                .andRespond(withStatus(HttpStatus.ACCEPTED));

        client.submitForWithdrawals(
                new WithdrawAllObjectionsResponse().companyNumber(COMPANY_NUMBER),
                message(EventType.WITHDRAWAL));

        server.verify();
    }

    @Test
    void shouldHandleExceptionWhenKafkaApiReturnsError() {

        server.expect(requestTo(CALLBACK_URL))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withServerError());

        WithdrawAllObjectionsResponse response = new WithdrawAllObjectionsResponse();
        response.setCompanyNumber("12345678");

        StrikeOffPartnerObjections message = new StrikeOffPartnerObjections();
        message.setStrikeOffEventId("event-123");
        message.setCompanyNumber("12345678");

        assertDoesNotThrow(() ->
                client.submitForWithdrawals(response, message));

        server.verify();
    }

    private static StrikeOffPartnerObjections message(EventType eventType) {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-100")
                .setEventTime("2026-10-16T00:00:00Z")
                .setPartnerOrganisation("HMRC")
                .setEventType(eventType)
                .setCompanyNumber(COMPANY_NUMBER)
                .setStrikeOffEventId(STRIKE_OFF_EVENT_ID)
                .build();
    }


}