package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.sdk.manager.ApiSdkManager;
import uk.gov.companieshouse.api.InternalApiClient;


@Configuration
public class InternalApiClientConfig {
    @Bean
    public InternalApiClient internalApiClient() {
         return ApiSdkManager.getPrivateSDK();
    }

    @Bean
    public RestTemplate chipsRestTemplate() {
        return new RestTemplate();
    }
}