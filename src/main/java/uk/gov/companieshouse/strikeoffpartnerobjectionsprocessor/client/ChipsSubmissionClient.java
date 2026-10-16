package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import uk.gov.companieshouse.api.objections.model.BaseObjectionResponse;
import uk.gov.companieshouse.api.objections.model.WithdrawAllObjectionsResponse;
import uk.gov.companieshouse.strikeoff.partner.objections.StrikeOffPartnerObjections;

/**
 * Interface for submitting processed strike-off partner objections to the CHIPS system.
 * Implementations of this interface can handle the submission of objections and withdrawals
 * to the appropriate endpoints in the Real CHIPS system or Mock Chips.
 */
public interface ChipsSubmissionClient {
    void submitForObjections(BaseObjectionResponse response, StrikeOffPartnerObjections message);

    void submitForWithdrawals(WithdrawAllObjectionsResponse response, StrikeOffPartnerObjections message);
}
