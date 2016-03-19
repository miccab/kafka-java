package miccab.kafka.producer.simple;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by michal on 19.03.16.
 */
public class SimpleCallbackOnCompletion implements Callback {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleCallbackOnCompletion.class);
    private final String key;

    public SimpleCallbackOnCompletion(String key) {
        this.key = key;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if(recordMetadata != null) {
            LOG.info("message with key({}) sent to partition({}), offset({})", key, recordMetadata.partition(), recordMetadata.offset());
        } else {
            LOG.error("error while sending", exception);
        }
    }
}
