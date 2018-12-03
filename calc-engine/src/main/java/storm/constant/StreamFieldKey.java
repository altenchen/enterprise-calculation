package storm.constant;

import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.jetbrains.annotations.Contract;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-07-19
 * @description:
 */
public final class StreamFieldKey {

    public static final String VEHICLE_ID = "VID";

    public static final String KAFKA_MESSAGE = FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE;

    public static final String DATA = "DATA";

    public static final String CACHE = "CACHE";

    public static final String NOTICE = "NOTICE";

    public static final String FLAG = "FLAG";
}
