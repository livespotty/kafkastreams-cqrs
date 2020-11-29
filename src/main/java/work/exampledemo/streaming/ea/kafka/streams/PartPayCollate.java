package work.exampledemo.streaming.ea.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import java.time.Instant;
import java.util.Properties;

public class PartPayCollate {

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "participant-collate-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // using offset = 'earliest' - for demo purposes only.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        //  config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,2000);
        //  config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,800);

        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);


        StreamsBuilder builder = new StreamsBuilder();
        // Getting the stream to construct Ktable view 
        KStream<String, JsonNode> bankTransactions =
                builder.stream("payment-item-transactions", Consumed.with( Serdes.String(), jsonSerde));


        // create the initial json object for balances
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("transtype", "init");
        initialBalance.put("balance", 10000);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());
    
        // Building Materialized view
        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,   
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.with(Serdes.String(), jsonSerde)
                        
                );
        // Building resultant stream
        bankBalance.toStream().to("participant-collate-log",Produced.with( Serdes.String(), jsonSerde)) ; 
        
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("transtype", transaction.get("transtype").asText());
        newBalance.put("amount", transaction.get("amount").asText());
        
        if (transaction.get("crdr").asInt() != 0 ) 
        {
            newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());
            newBalance.put("crdr", "CR");
        } 
        else
        {
            newBalance.put("balance", balance.get("balance").asInt() - transaction.get("amount").asInt());
            newBalance.put("crdr","DR");
        }

        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        Instant now = Instant.now();
        // This is the processed time, when the balance is re-calculated
        newBalance.put("processtime", now.toString());
        return newBalance;
    }
}
