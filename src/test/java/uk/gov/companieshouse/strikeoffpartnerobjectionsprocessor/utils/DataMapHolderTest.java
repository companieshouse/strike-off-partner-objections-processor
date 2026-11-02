package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.logging.util.DataMap;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataMapHolderTest {

    @AfterEach
    void clearContext() {
        DataMapHolder.clear();
    }

    @Test
    void gettersReturnEmptyContextWhenNoneIsInstalled() {
        DataMapHolder.clear();

        assertEquals(Map.of(), DataMapHolder.get().getLogMap());
        assertEquals(Map.of(), DataMapHolder.getLogMap());
    }

    @Test
    void gettersReturnInstalledContext() {
        DataMap dataMap = new DataMap.Builder().topic("topic").build();
        Map<String, Object> logMap = Map.of("event_id", "event-123");
        DataMapHolder.set(dataMap, logMap);

        assertEquals(dataMap, DataMapHolder.get());
        assertEquals(logMap, DataMapHolder.getLogMap());
    }
}
