package storm.bolt.mapper;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-08-12
 * @description:
 */
public final class IndexNameBasedTupleToKafkaMapper<K,V> implements TupleToKafkaMapper<K, V> {

    private static final long serialVersionUID = -562540927661143309L;

    public static final int BOLT_KEY = 1;
    public static final int BOLT_MESSAGE = 2;
    public int boltKeyIndex;
    public int boltMessageIndex;

    public IndexNameBasedTupleToKafkaMapper() {
        this(BOLT_KEY, BOLT_MESSAGE);
    }

    public IndexNameBasedTupleToKafkaMapper(int boltKeyIndex, int boltMessageIndex) {
        this.boltKeyIndex = boltKeyIndex;
        this.boltMessageIndex = boltMessageIndex;
    }

    @Override
    public K getKeyFromTuple(Tuple tuple) {
        //for backward compatibility, we return null when key is not present.
        return tuple.size() > boltKeyIndex ? (K) tuple.getValue(boltKeyIndex) : null;
    }

    @Override
    public V getMessageFromTuple(Tuple tuple) {
        //for backward compatibility, we return null when message is not present.
        return tuple.size() > boltMessageIndex ? (V) tuple.getValue(boltMessageIndex) : null;
    }
}
