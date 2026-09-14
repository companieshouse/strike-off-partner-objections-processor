package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import uk.gov.companieshouse.logging.util.DataMap;

import java.util.Map;
public final class StrikeOffPartnerEventsProcessorConstants {
    public static final String APPLICATION_NAMESPACE = "strike-off-partner-objections-processor";
    public static final String INTERNAL_COMPANY_URI = "/internal/company/";
    public static final String OBJECTIONS = "strike-off-partner-objections";
    public static final String WITHDRAWALS = "strike-off-partner-objections-withdrawals";
    public static final String STATUS = "status";
    public static final String WITHDRAWAL_STATUS = "withdrawal-status";
    public static final String CHIPS_PARTNER_OBJECTIONS_ENDPOINT = "/chipsgeneric/strike-off-partner-objections";
    public static final String HMRC_CALLBACK_UPDATE_ENDPOINT = "/internal/callback-update";
    public static final String CALLBACK_KIND_OBJECTION = "strike-off-partner-objection#objection";
    public static final String CALLBACK_KIND_WITHDRAWAL = "strike-off-partner-objection#withdrawal";

    private StrikeOffPartnerEventsProcessorConstants() {
        /* This utility class should not be instantiated */
    }

    public static Map<String, Object> buildBaseKafkaLogMap(ConsumerRecord<String, ?> baseRecord) {
        return new DataMap.Builder()
                .topic(baseRecord.topic())
                .partition(baseRecord.partition())
                .offset(baseRecord.offset())
                .build()
                .getLogMap();
    }
}
