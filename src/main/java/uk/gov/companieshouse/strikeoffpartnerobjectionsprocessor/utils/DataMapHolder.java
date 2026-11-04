package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils;

import uk.gov.companieshouse.logging.util.DataMap;

import java.util.Map;

/**
 * Holds structured logging context for the current Kafka listener thread.
 */
public final class DataMapHolder {

    private static final ThreadLocal<DataMap.Builder> CONTEXT =
            ThreadLocal.withInitial(DataMap.Builder::new);

    private DataMapHolder() {
    }

    /**
     * Returns a snapshot of the current context, or an empty DataMap outside listener processing.
     */
    public static DataMap get() {
        return getBuilder().build();
    }

    /**
     * Returns the current context as a log map.
     */
    public static Map<String, Object> getLogMap() {
        return get().getLogMap();
    }

    /**
     * Returns the current thread's builder for populating structured context.
     */
    public static DataMap.Builder getBuilder() {
        return CONTEXT.get();
    }

    /**
     * Removes the context before the listener thread processes another message.
     */
    public static void clear() {
        CONTEXT.remove();
    }
}
