package work.exampledemo.streaming.ea.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PaymentTransactionsProducerTests {

    @Test
    public void newRandomTransactionsTest(){
        ProducerRecord<String, String> record = PaymentTransactionsProducer.newRandomTransaction("participant-id-001");
        String key = record.key();
        String value = record.value();

        assertEquals("participant-id-001", key);

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(value);
            assertEquals("participant-id-001", node.get("participant").asText());
            assertTrue("Amount should be less than 100", node.get("amount").asInt() < 100);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(value);

    }

}
