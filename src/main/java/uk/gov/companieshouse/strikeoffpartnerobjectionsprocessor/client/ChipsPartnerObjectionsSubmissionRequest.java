package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import jakarta.annotation.Nullable;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

public record ChipsPartnerObjectionsSubmissionRequest(
        String company_number,
        String submission_company_name,
        String source,
        String partner_case_reference,
        String partner_objection_workstream,
        String partner_contact_email,
        @Nullable String partner_objection_reason,
        String strike_off_event_id
) {
    static ChipsPartnerObjectionsSubmissionRequest from(BaseObjectionResponse response, StrikeOffPartnerObjections message) {
        return new ChipsPartnerObjectionsSubmissionRequest(
                response.getCompanyNumber(),
                response.getSubmissionCompanyName(),
                message.getPartnerOrganisation(),
                response.getPartnerCaseReference(),
                response.getPartnerObjectionWorkstream(),
                response.getPartnerContactEmail(),
                response.getPartnerObjectionReason().toString(),
                response.getObjectionId()
        );
    }

    static ChipsPartnerObjectionsSubmissionRequest from(WithdrawAllObjectionsResponse response, StrikeOffPartnerObjections message) {
        return new ChipsPartnerObjectionsSubmissionRequest(
                response.getCompanyNumber(),
                response.getSubmissionCompanyName(),
                message.getPartnerOrganisation(),
                response.getPartnerCaseReference(),
                response.getPartnerObjectionWorkstream(),
                response.getPartnerContactEmail(),
                null,
                response.getWithdrawalId()
        );
    }
}
