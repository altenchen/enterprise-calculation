package storm.kafka.bolt;

import storm.util.ConfigUtils;

/**
 * @author: xzp
 * @date: 2018-08-22
 * @description: 这个类是否还有在用， 如果没有的话，在下一个版本的时候删除咯 - 许智杰 2018-10-23
 */
@Deprecated
public final class KafkaBoltTopic {

    private static final String DEFAULT_NOTICE_TOPIC = "notice_topic";

    public static final String NOTICE_TOPIC;

    static {
        NOTICE_TOPIC = ConfigUtils.getSysDefine().getKafkaProducerVehicleNoticeTopic();
    }
}
