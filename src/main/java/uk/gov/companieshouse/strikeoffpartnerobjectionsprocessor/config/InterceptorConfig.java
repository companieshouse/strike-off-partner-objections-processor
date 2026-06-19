package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.interceptor.AuthenticationInterceptor;

@Configuration
@ComponentScan("uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.interceptor")
public class InterceptorConfig implements WebMvcConfigurer {

    public static final String HEALTH_CHECK = "**/healthcheck";
    public static final String SPECIFIC_HEALTH_CHECK = "/strike-off-partner-objections-processor/healthcheck";
    public static final String SPECIFIC_HEALTH_CHECK_TRAILING_SLASH = "/strike-off-partner-objections-processor/healthcheck/";
    
    
    private final AuthenticationInterceptor authenticationInterceptor;

    public InterceptorConfig(AuthenticationInterceptor authenticationInterceptor) {
        this.authenticationInterceptor = authenticationInterceptor;
    }

    /**
     * Set up the interceptors to run against endpoints when the endpoints are called
     * Interceptors are executed in the order they are added to the registry
     * @param registry The spring interceptor registry
     */
    @Override
    public void addInterceptors(@NonNull InterceptorRegistry registry) {
        addAuthenticationInterceptor(registry);
    }

    /**
     * Interceptor to authenticate requests to all endpoints except the health check endpoint
     * @param registry The spring interceptor registry
     */
    private void addAuthenticationInterceptor(InterceptorRegistry registry) {
        registry.addInterceptor(authenticationInterceptor)
                .excludePathPatterns(HEALTH_CHECK)
                .excludePathPatterns(SPECIFIC_HEALTH_CHECK)
                .excludePathPatterns(SPECIFIC_HEALTH_CHECK_TRAILING_SLASH);
    }
    
}
