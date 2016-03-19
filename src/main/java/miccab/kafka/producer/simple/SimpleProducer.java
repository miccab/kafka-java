package miccab.kafka.producer.simple;

import miccab.kafka.producer.ProducerRecordProvider;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created by michal on 19.03.16.
 */
public class SimpleProducer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);

    public static void main(String [] args) throws URISyntaxException {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "SimpleProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        final Producer<String, String> kafkaProducer = new KafkaProducer<>(props);

        final String topic = args[0];

        final Iterator<ProducerRecord<String, String>> records = ProducerRecordProvider.fromCsvFie(new File(SimpleProducer.class.getResource("/data.csv").toURI()),
                                                                                                   topic);
        records.forEachRemaining(record -> kafkaProducer.send(record, new SimpleCallbackOnCompletion(record.key())));
        kafkaProducer.close();
    }
}
