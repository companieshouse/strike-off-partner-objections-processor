package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.sdk.manager.ApiSdkManager;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class InternalApiClientConfigTest {

    @Test
    void internalApiClient_returnsDelegateFromApiSdkManager() {
        InternalApiClient mockClient = mock(InternalApiClient.class);
        InternalApiClientConfig config = new InternalApiClientConfig();

        try (MockedStatic<ApiSdkManager> sdkManager = mockStatic(ApiSdkManager.class)) {
            sdkManager.when(ApiSdkManager::getPrivateSDK).thenReturn(mockClient);

            InternalApiClient result = config.internalApiClient();

            assertSame(mockClient, result);
            sdkManager.verify(ApiSdkManager::getPrivateSDK);
        }
    }
}

