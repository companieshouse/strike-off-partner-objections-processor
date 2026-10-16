package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsPartnerObjectionsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.ChipsSubmissionClient;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client.MockChipsPartnerObjectionsSubmissionClient;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

@Tag("integration-test")
@ActiveProfiles("stagsbox")
@SpringBootTest(properties = {
        "chips.mock.enabled=true",
        "chips.mock.kafka.api.key=test-key",
        "chips.mock.kafka.api.url=http://localhost/private/strike-off-partner-objections-processed",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka(partitions = 1, topics = {
        IntegrationTestFixtures.INCOMING_TOPIC,
        IntegrationTestFixtures.PROCESSED_TOPIC
})
class MockChipsProfileIntegrationTest {

    private static final String CALLBACK_URL =
            "http://localhost/private/strike-off-partner-objections-processed";

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ChipsSubmissionClient submissionClient;

    @Autowired
    private RestTemplate restTemplate;

    @MockitoBean
    private InternalApiClient internalApiClient;

    @Test
    void stagsboxWithFlagEnabledUsesMockAndPostsSuccessfulObjection() {
        assertInstanceOf(MockChipsPartnerObjectionsSubmissionClient.class, submissionClient);
        assertTrue(context.getBeansOfType(ChipsPartnerObjectionsSubmissionClient.class).isEmpty());

        MockRestServiceServer server = MockRestServiceServer.bindTo(restTemplate).build();
        server.expect(requestTo(CALLBACK_URL))
                .andExpect(method(HttpMethod.POST))
                .andExpect(header(HttpHeaders.AUTHORIZATION, "Basic test-key"))
                .andExpect(jsonPath("$.strike_off_event_id")
                        .value(IntegrationTestFixtures.STRIKE_OFF_EVENT_ID))
                .andExpect(jsonPath("$.event_type").value("OBJECTION"))
                .andExpect(jsonPath("$.success_failure_indicator").value("SUCCESS"))
                .andRespond(withSuccess());

        submissionClient.submitForObjections(
                new BaseObjectionResponse()
                        .companyNumber(IntegrationTestFixtures.COMPANY_NUMBER),
                IntegrationTestFixtures.incomingMessage(EventType.OBJECTION));

        server.verify();
    }

    @Test
    void stagsboxWithFlagEnabledPostsSuccessfulWithdrawal() {
        MockRestServiceServer server = MockRestServiceServer.bindTo(restTemplate).build();
        server.expect(requestTo(CALLBACK_URL))
                .andExpect(method(HttpMethod.POST))
                .andExpect(jsonPath("$.event_type").value("WITHDRAWAL"))
                .andExpect(jsonPath("$.success_failure_indicator").value("SUCCESS"))
                .andRespond(withSuccess());

        submissionClient.submitForWithdrawals(
                new WithdrawAllObjectionsResponse()
                        .companyNumber(IntegrationTestFixtures.COMPANY_NUMBER),
                IntegrationTestFixtures.incomingMessage(EventType.WITHDRAWAL));

        server.verify();
    }
}
