package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.api.chskafka.ProcessedStrikeOffPartnerObjection;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static java.time.ZoneOffset.UTC;
import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerEventsProcessorConstants.LOG;

/**
 * Bypasses CHIPS processing and posts a mock successful response to
 * chs-kafka-api-java, which publishes it to the
 * strike-off-partner-objections-processed topic.
 *
 * <p>Used for testing in stagsbox and livesbox when the mock feature flag is enabled.
 */
@Component
@Profile({"stagsbox", "livesbox"})
@ConditionalOnProperty(name = "chips.mock.enabled", havingValue = "true")
public class MockChipsPartnerObjectionsSubmissionClient implements ChipsSubmissionClient {
    private final RestTemplate restTemplate;
    private final String kafkaApiUrl;
    private final String kafkaApiKey;

    public MockChipsPartnerObjectionsSubmissionClient(
            RestTemplate restTemplate,
            @Value("${chips.mock.kafka.api.url}") String kafkaApiUrl,
            @Value("${chips.mock.kafka.api.key}") String kafkaApiKey) {
        this.restTemplate = restTemplate;
        this.kafkaApiUrl = kafkaApiUrl;
        this.kafkaApiKey = kafkaApiKey;
    }

    @PostConstruct
    public void logConfiguration() {
        LOG.info("CHIPS mock integration enabled. kafka api URL=" + kafkaApiUrl);
    }

    @Override
    public void submitForObjections(BaseObjectionResponse response, StrikeOffPartnerObjections message) {
        postProcessed(new ProcessedStrikeOffPartnerObjection().strikeOffEventId(
                message.getStrikeOffEventId())
                .eventType(ProcessedStrikeOffPartnerObjection.EventTypeEnum.OBJECTION)
                .companyNumber(response.getCompanyNumber())
                .successFailureIndicator(ProcessedStrikeOffPartnerObjection.SuccessFailureIndicatorEnum.SUCCESS)
                .initialExpirationOn(LocalDate.now(UTC).plusYears(2)));
    }

    @Override
    public void submitForWithdrawals(WithdrawAllObjectionsResponse response, StrikeOffPartnerObjections message) {
        postProcessed(new ProcessedStrikeOffPartnerObjection()
                .strikeOffEventId(message.getStrikeOffEventId())
                .eventType(ProcessedStrikeOffPartnerObjection.EventTypeEnum.WITHDRAWAL)
                .companyNumber(response.getCompanyNumber())
                .successFailureIndicator(ProcessedStrikeOffPartnerObjection.SuccessFailureIndicatorEnum.SUCCESS)
                .initialExpirationOn(null));
    }

    private void postProcessed(ProcessedStrikeOffPartnerObjection processed) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(kafkaApiKey, "");
            restTemplate.postForEntity(
                    kafkaApiUrl, new HttpEntity<>(processed, headers), Void.class);
        } catch (Exception e) {
            LOG.error("Failed to post processed objection to mock CHIPS API", e);
        }
    }
}
