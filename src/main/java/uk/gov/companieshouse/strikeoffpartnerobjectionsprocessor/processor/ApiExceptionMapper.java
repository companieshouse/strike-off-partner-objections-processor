package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.processor;

import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.exceptions.InvalidStrikeOffMessageException;

/**
 * Maps API-related checked exceptions to runtime exceptions so callers can control retry behaviour.
 */
final class ApiExceptionMapper {

    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;

    private ApiExceptionMapper() {
        // Utility class.
    }

    static RuntimeException map(String operationName, String eventId, Exception ex, Logger logger) {
        if (ex instanceof URIValidationException) {
            return new InvalidStrikeOffMessageException(
                    String.format("Non-retryable URI validation error for eventId=%s", eventId), ex);
        }

        if (ex instanceof ApiErrorResponseException apiErrorResponseException) {
            return mapApiStatusError(operationName, eventId, apiErrorResponseException, ex, logger);
        }

        return new RuntimeException(String.format("Retryable error for eventId=%s", eventId), ex);
    }

    private static RuntimeException mapApiStatusError(String operationName,
                                                      String eventId,
                                                      ApiErrorResponseException apiErrorResponseException,
                                                      Exception ex,
                                                      Logger logger) {
        int status = apiErrorResponseException.getStatusCode();
        logger.info(String.format("%s failed: status=%d, eventId=%s", operationName, status, eventId));

        if (isNonRetryableClientError(status)) {
            return new InvalidStrikeOffMessageException(
                    String.format("Non-retryable API error (status=%d) for eventId=%s", status, eventId), ex);
        }

        return new RuntimeException(
                String.format("Retryable API error (status=%d) for eventId=%s", status, eventId), ex);
    }

    private static boolean isNonRetryableClientError(int status) {
        return status >= HTTP_BAD_REQUEST
                && status < HTTP_INTERNAL_SERVER_ERROR
                && status != HTTP_TOO_MANY_REQUESTS;
    }
}

