package storm.kafka.bolt;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.system.SysDefine;
import storm.util.ConfigUtils;

import java.util.Properties;

/**
 * @author: xzp
 * @date: 2018-08-22
 * @description:
 */
public final class KafkaBoltTopic {

    private static final String DEFAULT_NOTICE_TOPIC = "notice_topic";

    public static final String NOTICE_TOPIC;

    static {
        final ConfigUtils configUtils = ConfigUtils.getInstance();
        final Properties sysDefine = configUtils.sysDefine;

        final String noticeTopic = sysDefine.getProperty(SysDefine.KAFKA_TOPIC_NOTICE);
        NOTICE_TOPIC = StringUtils.defaultIfEmpty(noticeTopic, DEFAULT_NOTICE_TOPIC);
    }
}
