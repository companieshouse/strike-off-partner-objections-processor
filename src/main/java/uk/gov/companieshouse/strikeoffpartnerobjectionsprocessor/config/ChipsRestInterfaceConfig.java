package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;

@Configuration
public class ChipsRestInterfaceConfig {

    @Value("${chips.rest.interface.api-key}")
    private String chipsRestApiKey;

    @Bean
    public RestTemplate chipsRestTemplate() {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.getInterceptors().add(apiKeyInterceptor());
        return restTemplate;
    }

    private ClientHttpRequestInterceptor apiKeyInterceptor() {
        return (request, body, execution) -> {
            request.getHeaders().set("chs-api-key", chipsRestApiKey);
            request.getHeaders().set("Content-Type", "application/json");
            return execution.execute(request, body);
        };
    }
}
