package uk.gov.companieshouse.strikeoff.partner.objections.processed;

import java.time.LocalDate;

/**
 * Processing outcome event for strike-off partner objection/withdrawal requests.
 */
public class StrikeOffPartnerObjectionsProcessed {
    private String strikeOffEventId;
    private SuccessFailureIndicator successFailureIndicator;
    private String errorMessage;
    private EventType eventType;
    private LocalDate initialExpirationOn;
    private String companyNumber;

    public StrikeOffPartnerObjectionsProcessed() {
    }

    public StrikeOffPartnerObjectionsProcessed(
            String strikeOffEventId,
            SuccessFailureIndicator successFailureIndicator,
            String errorMessage,
            EventType eventType,
            LocalDate initialExpirationOn,
            String companyNumber) {
        this.strikeOffEventId = strikeOffEventId;
        this.successFailureIndicator = successFailureIndicator;
        this.errorMessage = errorMessage;
        this.eventType = eventType;
        this.initialExpirationOn = initialExpirationOn;
        this.companyNumber = companyNumber;
    }

    public String getStrikeOffEventId() {
        return strikeOffEventId;
    }

    public void setStrikeOffEventId(String strikeOffEventId) {
        this.strikeOffEventId = strikeOffEventId;
    }

    public SuccessFailureIndicator getSuccessFailureIndicator() {
        return successFailureIndicator;
    }

    public void setSuccessFailureIndicator(SuccessFailureIndicator successFailureIndicator) {
        this.successFailureIndicator = successFailureIndicator;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public LocalDate getInitialExpirationOn() {
        return initialExpirationOn;
    }

    public void setInitialExpirationOn(LocalDate initialExpirationOn) {
        this.initialExpirationOn = initialExpirationOn;
    }

    public String getCompanyNumber() {
        return companyNumber;
    }

    public void setCompanyNumber(String companyNumber) {
        this.companyNumber = companyNumber;
    }
}

