
## Solve complex business problems using simple event sourcing implementation and CQRS pattern with Kafka Streams

### Intraday liquidity management and monitoring 

- Monitor and measure expected daily gross liquidity in-flows and outflows.

- Monitor and alert intraday liquidity exposures against the available liquidity sources.

- Manage and mobilize sufficient liquidity funding sources to meet the liquidity de-mands and avoid shortfalls. 


References: 

- https://www.softwareag.com/au/images/White%20Paper%20Intraday%20Liquidity%20Monitoring_tcm390-169380.pdf

- http://www.riskpro.in/blog/admin/intra-day-liquidity-risk-management

- https://www.confluent.io/blog/event-sourcing-cqrs-stream-processing-apache-kafka-whats-connection/
 

### Pre-Requisities
- JDK 11 or 8
- Maven 3.8.x

### Tech stack

- Java
- Kafka 2.6
- Kafka Streaming API

### How to Build and Run
mvn clean install

### Running Kafka Brokers

Use the following link to install and run Kafka in few minutes

https://kafka.apache.org/quickstart

### This exampledemo work project consists for two parts

- Participant Intraday liquidity Managment Program (participant-payment-collateral)
- Various Simulators

### Run this to Produce random stream of payment transaction (Simulator)
``` java
java -cp target/participant-payment-collateral-1.0-SNAPSHOT-jar-with-dependencies.jar work.exampledemo.streaming.ea.kafka.streams.PaymentTransactionsProducer
``` 
### Run this to create CQRS process to stream the data to Ktable. The final log is stream of Participant Payment Collateral running Balance (Main Program)
``` java
java -cp target/participant-payment-collateral-1.0-SNAPSHOT-jar-with-dependencies.jar work.exampledemo.streaming.ea.kafka.streams.PartPayCollate
```
### Run this to create Random Settlement Transaction for the Participant (Simulator)
``` java
java -cp target/participant-payment-collateral-1.0-SNAPSHOT-jar-with-dependencies.jar work.exampledemo.streaming.ea.kafka.streams.SettleTransProducer
```
### Run this to create Random Funding or De-funding Transaction for the Participant (Simulator)
``` java
java -cp target/participant-payment-collateral-1.0-SNAPSHOT-jar-with-dependencies.jar work.exampledemo.streaming.ea.kafka.streams.FundingDefunding
```
### Run the console consumer to view the participant collateral balance log (Final Output)
``` bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic participant-collate-log
```