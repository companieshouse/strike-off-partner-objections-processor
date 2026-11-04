package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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
        DataMapHolder.getBuilder()
                .topic("topic")
                .companyNumber("12345678")
                .requestId("request-123");

        assertEquals(DataMapHolder.get().getLogMap(), DataMapHolder.getLogMap());
        assertEquals("12345678", DataMapHolder.getLogMap().get("company_number"));
        assertEquals("request-123", DataMapHolder.getLogMap().get("request_id"));
    }
}
