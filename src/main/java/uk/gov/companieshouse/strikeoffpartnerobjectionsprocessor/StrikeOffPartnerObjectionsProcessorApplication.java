package uk.gov.companieshouse.strikeoffpartnerobjectionsprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class StrikeOffPartnerObjectionsProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(StrikeOffPartnerObjectionsProcessorApplication.class, args);
    }
}
