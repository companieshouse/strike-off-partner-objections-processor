package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils;

import uk.gov.companieshouse.logging.util.DataMap;

/**
 * Message metadata shared by synchronous processing on the Kafka listener thread.
 */
public final class DataMapHolder {
    public static final String KAFKA_MESSAGE_RECEIVED = "kafka_message_received";
    public static final String PROCESSING_STARTED = "processing_started";
    public static final String PROCESSING_COMPLETED = "processing_completed";
    public static final String PROCESSING_FAILED = "processing_failed";
    public static final String PROCESSING_SKIPPED = "processing_skipped";
    public static final String VALIDATION_STARTED = "validation_started";
    public static final String VALIDATION_COMPLETED = "validation_completed";
    public static final String INTERNAL_API_GET_OBJECTION_REQUEST = "internal_api_get_objection_request";
    public static final String INTERNAL_API_GET_OBJECTION_RESPONSE = "internal_api_get_objection_response";
    public static final String INTERNAL_API_GET_WITHDRAWAL_REQUEST = "internal_api_get_withdrawal_request";
    public static final String INTERNAL_API_GET_WITHDRAWAL_RESPONSE = "internal_api_get_withdrawal_response";
    public static final String INTERNAL_API_UPDATE_OBJECTION_REQUEST = "internal_api_update_objection_request";
    public static final String INTERNAL_API_UPDATE_OBJECTION_RESPONSE = "internal_api_update_objection_response";
    public static final String INTERNAL_API_UPDATE_WITHDRAWAL_REQUEST = "internal_api_update_withdrawal_request";
    public static final String INTERNAL_API_UPDATE_WITHDRAWAL_RESPONSE = "internal_api_update_withdrawal_response";
    public static final String CHIPS_REQUEST = "chips_request";
    public static final String CHIPS_RESPONSE = "chips_response";
    public static final String PROCESSED_OUTCOME_RECEIVED = "processed_outcome_received";
    public static final String RETRY = "retry";

    private static final ThreadLocal<DataMap.Builder> BUILDER = new ThreadLocal<>();

    private DataMapHolder() {
    }

    public static void initialise(DataMap.Builder builder) {
        BUILDER.set(builder);
    }

    /**
     * Returns the shared listener builder without clearing metadata, or a detached builder outside a listener.
     */
    public static DataMap.Builder get() {
        DataMap.Builder builder = BUILDER.get();
        return builder == null ? new DataMap.Builder() : builder;
    }

    /**
     * Clears transient fields before an unrelated log entry, retaining shared message metadata.
     */
    public static void clearLogFields() {
        get().operation(null)
                .resourceKind(null)
                .resourceUri(null)
                .status(null)
                .errorType(null)
                .retryReason(null);
    }

    public static void clear() {
        BUILDER.remove();
    }
}
