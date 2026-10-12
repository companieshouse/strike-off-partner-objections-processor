# strike-off-partner-objections-processor
Kafka processor service for consuming strike-off objections and withdrawals, integrating with CHIPS and sending callback notification to partner organisations (e.g. HMRC).
Consumer to listen to strike-off-partner-objections-incoming topic and strike-off-partner-objections-processed topic, and process the messages accordingly.
---
## Related Services

- [strike-off-partner-objections-processor](https://github.com/companieshouse/strike-off-partner-objections-processor)

## Technology Stack
- Java 21
- Spring Boot
- Maven
- MongoDB
- Apache Kafka
---

## Requirements

To build the `strike-off-partner-objections-processor`, you will need:
* [Git](https://git-scm.com/downloads)
* [Java 21](https://www.oracle.com/uk/java/technologies/downloads/#java21)
* [Maven](https://maven.apache.org/download.cgi)
* Internal Companies House core services
