package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor.interceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Objects;

@Component
public class AuthenticationInterceptor implements HandlerInterceptor {

    /**
     * Ensure requests are authenticated for a user
     * For now just return true as the service is not yet secured and will be secured in a future sprint
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        return true;
    }
    
}
