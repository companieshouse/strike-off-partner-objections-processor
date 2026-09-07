package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.ObjectionProcessingStatus;
import uk.gov.companieshouse.api.objections.model.PartnerObjectionReason;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawalProcessingStatus;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

class ChipsPartnerObjectionsSubmissionClientTest {
    private static final String BASE_URL = "http://chips-rest-interfaces";
    private static final String ENDPOINT_URL = BASE_URL + "/chipsgeneric/strike-off-partner-objections";
    private static final String OBJECTION_ID = "obj-001";
    private static final String WITHDRAWAL_ID = "wd-001";

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
                .andExpect(jsonPath("$.company_number").value("12345678"))
                .andExpect(jsonPath("$.submission_company_name").value("TEST_ORG"))
                .andExpect(jsonPath("$.source").value("HMRC"))
                .andExpect(jsonPath("$.partner_case_reference").value("OBJECTION"))
                .andExpect(jsonPath("$.partner_contact_email").value("12345678"))
                .andExpect(jsonPath("$.partner_objection_reason").value("other"))
                .andExpect(jsonPath("$.strike_off_event_id").value("obj-001"))
                .andRespond(withStatus(HttpStatus.ACCEPTED));

        assertDoesNotThrow(() -> client.submitForObjections(createObjectionResponse(ObjectionProcessingStatus.OBJECTION_ACCEPTED), buildMessage(EventType.OBJECTION)));
        server.verify();
    }

    @Test
    void submit_throwsNonRetryableFor403Response() {
        server.expect(requestTo(ENDPOINT_URL))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.FORBIDDEN));
        StrikeOffPartnerObjections message = buildMessage(EventType.WITHDRAWAL);
        WithdrawAllObjectionsResponse baseResponse = new WithdrawAllObjectionsResponse();

        ChipsSubmissionException exception = assertThrows(
                ChipsSubmissionException.class,
                () -> client.submitForWithdrawals(baseResponse, message));

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
                () -> client.submitForWithdrawals(createWithdrawalResponse(WithdrawalProcessingStatus.WITHDRAWAL_ACCEPTED), message));

        assertEquals(200, exception.getStatusCode());
    }

    @Test
    void submit_usesStringResponseTypeForChipsCall() {
        RestTemplate restTemplate = mock(RestTemplate.class);
        ChipsPartnerObjectionsSubmissionClient submissionClient =
                new ChipsPartnerObjectionsSubmissionClient(restTemplate, BASE_URL);

        when(restTemplate.postForEntity(
                eq(ENDPOINT_URL),
                any(ChipsPartnerObjectionsSubmissionRequest.class),
                eq(String.class)))
                .thenReturn(ResponseEntity.accepted().build());

        assertDoesNotThrow(() -> submissionClient.submitForObjections(createObjectionResponse(ObjectionProcessingStatus.OBJECTION_ACCEPTED), buildMessage(EventType.OBJECTION)));

        verify(restTemplate).postForEntity(
                eq(ENDPOINT_URL),
                any(ChipsPartnerObjectionsSubmissionRequest.class),
                eq(String.class));
    }

    private StrikeOffPartnerObjections buildMessage(EventType eventType) {
        return StrikeOffPartnerObjections.newBuilder()
                .setEventId("evt-100")
                .setEventTime("2026-08-24T00:00:00Z")
                .setSource("HMRC")
                .setEventType(eventType)
                .setCompanyNumber("12345678")
                .setStrikeOffEventId("strike-100")
                .build();
    }

    private static BaseObjectionResponse createObjectionResponse(
            ObjectionProcessingStatus processingStatus) {
        return new BaseObjectionResponse()
                .companyNumber("12345678")
                .submissionCompanyName("TEST_ORG")
                .objectionId(OBJECTION_ID)
                .partnerCaseReference("OBJECTION")
                .partnerObjectionReason(PartnerObjectionReason.OTHER)
                .partnerContactEmail("12345678")
                .processingStatus(processingStatus);
    }

    private static WithdrawAllObjectionsResponse createWithdrawalResponse(
            WithdrawalProcessingStatus processingStatus) {
        return new WithdrawAllObjectionsResponse()
                .companyNumber("12345678")
                .submissionCompanyName("TEST_ORG")
                .withdrawalId(WITHDRAWAL_ID)
                .partnerCaseReference("WITHDRAWAL")
                .partnerContactEmail("12345678")
                .processingStatus(processingStatus);
    }
}
