package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.client;

public record HmrcOutcomeCallbackRequest(
        String resource_kind,
        String resource_id,
        String company_number,
        String resource_uri,
        String processing_outcome
) {
}

