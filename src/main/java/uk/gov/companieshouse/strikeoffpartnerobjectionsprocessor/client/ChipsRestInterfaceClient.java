package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.api.objections.model.CreateObjectionRequest;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerObjectionsProcessorConstants.APPLICATION_NAMESPACE;

@Component
public class ChipsRestInterfaceClient {

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);
    private static final String POST_OBJECTION_PATH = "http://chips-rest-interfaces:8080/chipsgeneric/strike-off-partner-objections";

    private final RestTemplate chipsRestTemplate;

    public ChipsRestInterfaceClient(@Qualifier("chipsRestTemplate") RestTemplate chipsRestTemplate) {
        this.chipsRestTemplate = chipsRestTemplate;
    }

    /**
     * Post an objection to CHIPS via chips-rest-interfaces.
     *
     * @param request the CreateObjectionRequest containing objection details
     * @throws Exception if the POST request fails
     */
    public void postObjection(CreateObjectionRequest request) {
        try {
            chipsRestTemplate.postForObject(POST_OBJECTION_PATH, request, Void.class);
            LOG.info("Successfully posted objection to chips-rest-interfaces");
        } catch (Exception e) {
            LOG.error("Failed to post objection to chips-rest-interfaces", e);
            throw new RuntimeException("Failed to post objection to CHIPS", e);
        }
    }
}
