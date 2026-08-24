package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

public record ChipsPartnerObjectionsSubmissionRequest(
        String eventId,
        String eventTime,
        String source,
        String eventType,
        String companyNumber,
        String partnerOrganisation,
        String strikeOffEventId
) {
    static ChipsPartnerObjectionsSubmissionRequest from(StrikeOffPartnerObjections message) {
        return new ChipsPartnerObjectionsSubmissionRequest(
                message.getEventId(),
                message.getEventTime(),
                message.getSource(),
                message.getEventType() == null ? null : message.getEventType().name(),
                message.getCompanyNumber(),
                message.getPartnerOrganisation(),
                message.getStrikeOffEventId()
        );
    }
}
