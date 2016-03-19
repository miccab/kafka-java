package miccab.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Created by michal on 19.03.16.
 */
public class ProducerRecordProvider {

    public static Iterator<ProducerRecord<String, String>> fromCsvFie(File csvFile, String topic) {
        try {
            return Files.lines(Paths.get(csvFile.toURI()))
                    .filter(line -> line.length() > 0)
                    .map(line -> convertLineToProducerRecord(line, topic))
                    .iterator();
        } catch (IOException e) {
            throw new RuntimeException("error", e);
        }
    }

    private static ProducerRecord<String, String> convertLineToProducerRecord(String line, String topic) {
        final String [] columns = line.split(";");
        final String key = columns[0];
        final String value = columns[1];
        return new ProducerRecord<String, String>(topic, key, value);
    }
}
