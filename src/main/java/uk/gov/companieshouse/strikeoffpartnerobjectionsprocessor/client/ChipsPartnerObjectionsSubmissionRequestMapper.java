package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

@Component
public class ChipsPartnerObjectionsSubmissionRequestMapper {

    private static final String KIND_OBJECTION = "Objection";
    private static final String KIND_WITHDRAWAL = "Withdrawal";

    public ChipsPartnerObjectionsSubmissionRequest objectionRequest(
            final BaseObjectionResponse response,
            final StrikeOffPartnerObjections message) {
        return new ChipsPartnerObjectionsSubmissionRequest(
                response.getCompanyNumber(),
                response.getSubmissionCompanyName(),
                message.getPartnerOrganisation(),
                response.getPartnerCaseReference(),
                response.getPartnerObjectionWorkstream(),
                response.getPartnerContactEmail(),
                // partnerobjectionReason is mandatory for objections, but not for withdrawals, so we can safely call toString() here
                response.getPartnerObjectionReason().toString(),
                response.getObjectionId(),
                KIND_OBJECTION
        );
    }

    public ChipsPartnerObjectionsSubmissionRequest withdrawalRequest(
            final WithdrawAllObjectionsResponse response,
            final StrikeOffPartnerObjections message) {
        return new ChipsPartnerObjectionsSubmissionRequest(
                response.getCompanyNumber(),
                response.getSubmissionCompanyName(),
                message.getPartnerOrganisation(),
                response.getPartnerCaseReference(),
                response.getPartnerObjectionWorkstream(),
                response.getPartnerContactEmail(),
                null,
                response.getWithdrawalId(),
                KIND_WITHDRAWAL
        );
    }
}

