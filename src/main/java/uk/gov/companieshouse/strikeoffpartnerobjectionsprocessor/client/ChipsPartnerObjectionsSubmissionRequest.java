package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

import jakarta.annotation.Nullable;

public record ChipsPartnerObjectionsSubmissionRequest(
        String company_number,
        String submission_company_name,
        String source,
        String partner_case_reference,
        String partner_objection_workstream,
        String partner_contact_email,
        @Nullable String partner_objection_reason,
        String strike_off_event_id,
        String kind
) {}
