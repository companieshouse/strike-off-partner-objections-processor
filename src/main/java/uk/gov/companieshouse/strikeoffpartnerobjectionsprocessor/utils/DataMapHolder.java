package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils;

import uk.gov.companieshouse.logging.util.DataMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Holds structured logging context for the current Kafka listener thread.
 */
public final class DataMapHolder {

    private static final DataMap EMPTY_DATA_MAP = new DataMap.Builder().build();
    private static final ThreadLocal<LoggingContext> CONTEXT = new ThreadLocal<>();

    private DataMapHolder() {
    }

    /**
     * Returns the current DataMap, or an empty DataMap outside listener processing.
     */
    public static DataMap get() {
        LoggingContext context = CONTEXT.get();
        return context == null ? EMPTY_DATA_MAP : context.dataMap();
    }

    /**
     * Returns context including existing fields not supported by the library DataMap.
     */
    public static Map<String, Object> getLogMap() {
        LoggingContext context = CONTEXT.get();
        return context == null ? Map.of() : context.logMap();
    }

    /**
     * Installs a snapshot of the listener's structured context.
     */
    public static void set(DataMap dataMap, Map<String, Object> logMap) {
        Objects.requireNonNull(dataMap, "DataMap must not be null");
        Objects.requireNonNull(logMap, "Log map must not be null");
        CONTEXT.set(new LoggingContext(dataMap, Collections.unmodifiableMap(new HashMap<>(logMap))));
    }

    /**
     * Removes the context before the listener thread processes another message.
     */
    public static void clear() {
        CONTEXT.remove();
    }

    private record LoggingContext(DataMap dataMap, Map<String, Object> logMap) {
    }
}
