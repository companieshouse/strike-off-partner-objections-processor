package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.strikeoff.partner.objections.EventType;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

import static uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils.StrikeOffPartnerObjectionsProcessorConstants.APPLICATION_NAMESPACE;

@Component
public class StrikeOffPartnerObjectionsProcessor extends AbstractStrikeOffPartnerObjectionsProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAMESPACE);

    @Override
    protected boolean supports(EventType eventType) {
        return eventType == EventType.OBJECTION;
    }

    @Override
    protected void doProcess(StrikeOffPartnerObjections message) {
        LOG.info("Processing objection event with ID: " + message.getEventId());
    }
}
