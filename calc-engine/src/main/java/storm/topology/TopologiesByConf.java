package storm.topology;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.FieldNameTopicSelector;
import org.apache.storm.kafka.bolt.selector.KafkaTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.bolt.deal.cusmade.CarNoticelBolt;
import storm.bolt.deal.norm.*;
import storm.constant.StreamFieldKey;
import storm.kafka.spout.GeneralRecordTranslator;
import storm.kafka.spout.RegisterRecordTranslator;
import storm.stream.FromFilterToCarNoticeStream;
import storm.stream.FromGeneralToFilterStream;
import storm.stream.FromRegistToElasticsearchStream;
import storm.stream.KafkaStream;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;

import java.util.Properties;

/**
 * @author xzp
 */
public class TopologiesByConf {

    private static final Logger LOG = LoggerFactory.getLogger(TopologiesByConf.class);

    private static final ConfigUtils CONFIG_UTILS = ConfigUtils.getInstance();

    /**
     * http://storm.apache.org/releases/current/index.html
     * @param args 拓扑启动参数, 忽略.
     * @throws Exception 拓扑启动异常
     */
    public static void main(String[] args) throws Exception {

        Properties properties = CONFIG_UTILS.sysDefine;

        fillKafkaConf(properties);

        Config stormConf = buildStormConf(properties);
        StormTopology stormTopology = createTopology(properties);
        final String topologyName = properties.getProperty(SysDefine.TOPOLOGY_NAME, "qyallStorm");
        StormSubmitter.submitTopology(topologyName, stormConf, stormTopology);
    }

    private static Config buildStormConf(@NotNull Properties properties) {

        final int workerNo = Integer.valueOf(properties.getProperty("storm.worker.no"));

        final Config stormConf = readStormConf(properties);
        stormConf.setDebug(false);
        stormConf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);
        stormConf.setMaxSpoutPending(1000);
        stormConf.setNumWorkers(workerNo);
        //设置不需要应答
        stormConf.setNumAckers(0);
        stormConf.put(KafkaBolt.TOPIC, KafkaStream.TOPIC);

        return stormConf;
    }

    /**
     * 读取Storm相关配置
     *
     * @param properties 配置属性
     * @return Storm相关配置
     */
    private static Config readStormConf(@NotNull Properties properties) {

        final Config stormConf = new Config();

        //region alarm
        // 连续多少条报警才发送通知
        stormConf.put(SysDefine.ALARM_CONTINUE_COUNTS, properties.get(SysDefine.ALARM_CONTINUE_COUNTS));
        //endregion

        //region ctfo
        stormConf.put("ctfo.cacheDB", properties.getProperty("ctfo.cacheDB"));
        stormConf.put("ctfo.cacheHost", properties.getProperty("ctfo.cacheHost"));
        stormConf.put("ctfo.cachePort", properties.getProperty("ctfo.cachePort"));
        stormConf.put("ctfo.cacheTable", properties.getProperty("ctfo.cacheTable"));
        //endregion

        stormConf.put(SysDefine.DB_CACHE_FLUSH_TIME_SECOND, properties.get(SysDefine.DB_CACHE_FLUSH_TIME_SECOND));

        stormConf.put(SysDefine.ES_SEND_TIME, properties.get(SysDefine.ES_SEND_TIME));

        //region kafka
        stormConf.put("kafka.customer.hosts", properties.getProperty("kafka.broker.hosts"));
        stormConf.put("kafka.platform.veh.reg", properties.get("kafka.platform.veh.reg"));
        stormConf.put(SysDefine.KAFKA_TOPIC_ALARM, properties.getProperty(SysDefine.KAFKA_TOPIC_ALARM));
        stormConf.put(SysDefine.KAFKA_TOPIC_ALARM_STORE, properties.getProperty(SysDefine.KAFKA_TOPIC_ALARM_STORE));
        stormConf.put("kafka.topic.customfault", properties.get("kafka.topic.customfault"));
        stormConf.put(SysDefine.KAFKA_TOPIC_ES_STATUS, properties.get(SysDefine.KAFKA_TOPIC_ES_STATUS));
        stormConf.put("kafka.topic.fencealarm", properties.get("kafka.topic.fencealarm"));
        stormConf.put(SysDefine.KAFKA_TOPIC_NOTICE, properties.get(SysDefine.KAFKA_TOPIC_NOTICE));
        stormConf.put("kafka.topic.realinfostore", properties.getProperty("kafka.topic.realinfostore"));
        //endregion

        stormConf.put("offline.check.time", properties.get("offline.check.time"));

        //region redis
        stormConf.put("redis.cluster.data.syn", properties.get("redis.cluster.data.syn"));
        stormConf.put("redis.host", properties.getProperty("redis.host"));
        stormConf.put("redis.listenInterval", properties.getProperty("redis.listenInterval"));
        stormConf.put("redis.maxActive", properties.getProperty("redis.maxActive"));
        stormConf.put("redis.maxIdle", properties.getProperty("redis.maxIdle"));
        stormConf.put("redis.maxWait", properties.getProperty("redis.maxWait"));
        stormConf.put("redis.pass", properties.getProperty("redis.pass"));
        stormConf.put("redis.port", properties.getProperty("redis.port"));
        stormConf.put("redis.timeInterval", properties.getProperty("redis.timeInterval"));
        stormConf.put("redis.timeOut", properties.getProperty("redis.timeOut"));
        stormConf.put("redis.offline.checktime", properties.get("redis.offline.checktime"));
        stormConf.put("redis.offline.time", properties.get(StormConfigKey.REDIS_OFFLINE_SECOND));
        //endregion

        //region storm
        stormConf.put("storm.kafka.spout.no", properties.get("storm.kafka.spout.no"));
        stormConf.put("storm.worker.bolt.no", properties.get("storm.worker.bolt.no"));
        stormConf.put("storm.worker.no", properties.get("storm.worker.no"));
        //endregion

        // region notice.can
        stormConf.put(SysDefine.NOTICE_CAN_FAULT_TRIGGER_CONTINUE_COUNT, properties.getProperty(SysDefine.NOTICE_CAN_FAULT_TRIGGER_CONTINUE_COUNT));
        stormConf.put(SysDefine.NOTICE_CAN_FAULT_TRIGGER_TIMEOUT_MILLISECOND, properties.getProperty(SysDefine.NOTICE_CAN_FAULT_TRIGGER_TIMEOUT_MILLISECOND));
        stormConf.put(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_CONTINUE_COUNT, properties.getProperty(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_CONTINUE_COUNT));
        stormConf.put(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND, properties.getProperty(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND));
        // endregion

        // region jili
        stormConf.put(SysDefine.RULE_OVERRIDE, properties.getProperty(SysDefine.RULE_OVERRIDE));
        // endregion

        return stormConf;
    }

    /**
     * 创建 Storm 拓扑
     *
     * @param properties 配置属性
     * @return Storm 拓扑
     */
    private static StormTopology createTopology(@NotNull Properties properties) {

        final int realSpoutNo = Integer.valueOf(properties.getProperty("storm.kafka.spout.no"));
        final int boltNo = Integer.valueOf(properties.getProperty("storm.worker.bolt.no"));

        TopologyBuilder builder = new TopologyBuilder();

        buildKafkaSpout(builder, realSpoutNo);

        builderBlots(builder, boltNo);

        return builder.createTopology();
    }

    /**
     * @param builder     拓扑构建器
     * @param realSpoutNo Spout 基准并行度
     */
    private static void buildKafkaSpout(@NotNull TopologyBuilder builder, int realSpoutNo) {

        // kafka 实时报文消息
        final FromGeneralToFilterStream fromGeneralToFilterStream = FromGeneralToFilterStream.getInstance();
        final RecordTranslator generalTopicTranslator = new GeneralRecordTranslator();
        final KafkaSpoutConfig<String, String> generalTopicConfig = KafkaSpoutConfig
            .builder(SysDefine.KAFKA_BOOTSTRAP_SERVERS, SysDefine.VEH_REALINFO_DATA_TOPIC)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, SysDefine.VEH_REALINFO_GROUPID)
            .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
            .setRecordTranslator(generalTopicTranslator)
            .build();
        builder.setSpout(
            fromGeneralToFilterStream.getComponentId(),
            new KafkaSpout<>(generalTopicConfig),
            realSpoutNo
        );

        // kafka 平台注册报文消息
        final FromRegistToElasticsearchStream fromRegistToElasticsearchStream = FromRegistToElasticsearchStream.getInstance();
        final RecordTranslator registerTopicTranslator = new RegisterRecordTranslator();
        final KafkaSpoutConfig<String, String> registerTopicConfig = KafkaSpoutConfig
            .builder(SysDefine.KAFKA_BOOTSTRAP_SERVERS, SysDefine.PLAT_REG_TOPIC)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, SysDefine.PLAT_REG_GROUPID)
            .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
            .setRecordTranslator(registerTopicTranslator)
            .build();
        builder.setSpout(
            fromRegistToElasticsearchStream.getComponentId(),
            new KafkaSpout<>(registerTopicConfig),
            realSpoutNo
        );
    }

    /**
     * @param builder 拓扑构建器
     * @param boltNo  Blot 基准并行度
     */
    @SuppressWarnings("AlibabaMethodTooLong")
    private static void builderBlots(@NotNull final TopologyBuilder builder, final int boltNo) {

        final FromGeneralToFilterStream fromGeneralToFilterStream = FromGeneralToFilterStream.getInstance();
        builder
            .setBolt(
                SysDefine.CHECK_FILTER_BOLT_ID,
                new FilterBolt(),
                boltNo)
            .setNumTasks(boltNo * 3)
            // 接收车辆实时数据
            .fieldsGrouping(
                fromGeneralToFilterStream.getComponentId(),
                new Fields(StreamFieldKey.VEHICLE_ID));

        builder
            // 预警处理
            .setBolt(
                SysDefine.ALARM_BOLT_ID,
                new AlarmBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
            // 预警的车辆实时数据
            .fieldsGrouping(
                SysDefine.CHECK_FILTER_BOLT_ID,
                SysDefine.SPLIT_GROUP,
                new Fields(DataKey.VEHICLE_ID));

        builder
            // 故障处理
            .setBolt(
                SysDefine.FAULT_BOLT_ID,
                new FaultBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
            // 故障实时数据
            .fieldsGrouping(
                SysDefine.ALARM_BOLT_ID,
                SysDefine.FAULT_GROUP,
                new Fields(DataKey.VEHICLE_ID));

        builder
            // 电子围栏告警处理
            .setBolt(
                SysDefine.FENCE_BOLT_ID,
                new EleFenceBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
            // 电子围栏告警实时数据
            .fieldsGrouping(
                SysDefine.CHECK_FILTER_BOLT_ID,
                SysDefine.FENCE_GROUP,
                new Fields(DataKey.VEHICLE_ID));

        final FromFilterToCarNoticeStream fromFilterToCarNoticeStream = FromFilterToCarNoticeStream.getInstance();
        builder
            // soc 与超时处理
            .setBolt(
                SysDefine.CUS_NOTICE_BOLT_ID,
                new CarNoticelBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
            // soc 与超时处理实时数据
            .fieldsGrouping(
                fromFilterToCarNoticeStream.getComponentId(),
                fromFilterToCarNoticeStream.getStreamId(),
                new Fields(DataKey.VEHICLE_ID));

        final FromRegistToElasticsearchStream fromRegistToElasticsearchStream = FromRegistToElasticsearchStream.getInstance();
        builder
            // es数据同步处理
            .setBolt(
                SysDefine.SYNES_BOLT_ID,
                new SynEsculBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
            .fieldsGrouping(
                SysDefine.CHECK_FILTER_BOLT_ID,
                SysDefine.SYNES_GROUP,
                new Fields(DataKey.VEHICLE_ID))
            .noneGrouping(
                fromRegistToElasticsearchStream.getComponentId(),
                fromRegistToElasticsearchStream.getStreamId());

        buildKafkaBolt(builder, boltNo);
    }

    private static void buildKafkaBolt(
        @NotNull final TopologyBuilder builder,
        final int boltNo) {

        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SysDefine.KAFKA_BOOTSTRAP_SERVERS);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

        final KafkaTopicSelector selector = new FieldNameTopicSelector(KafkaStream.TOPIC, null);

        final TupleToKafkaMapper<String, String> mapper = new FieldNameBasedTupleToKafkaMapper<>(
            KafkaStream.BOLT_KEY,
            KafkaStream.BOLT_MESSAGE);

        final KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>()
            .withProducerProperties(producerProperties)
            .withTopicSelector(selector)
            .withTupleToKafkaMapper(mapper);

        builder
            // 发送kafka消息，必要时可以动态增加线程数以增加发送线程数据
            .setBolt(
                SysDefine.KAFKASEND_BOLT_ID,
                kafkaBolt,
                boltNo * 2)
            .setNumTasks(boltNo * 6)
            // 车辆告警数据
            .fieldsGrouping(
                SysDefine.ALARM_BOLT_ID,
                SysDefine.VEH_ALARM,
                new Fields(KafkaStream.BOLT_KEY))
            // 车辆报警状态、实时需要存储的数据
            .fieldsGrouping(
                SysDefine.ALARM_BOLT_ID,
                SysDefine.VEH_ALARM_REALINFO_STORE,
                new Fields(KafkaStream.BOLT_KEY))
            // 电子围栏
            .fieldsGrouping(
                SysDefine.FENCE_BOLT_ID,
                SysDefine.FENCE_ALARM,
                new Fields(KafkaStream.BOLT_KEY))
            // 故障处理
            .fieldsGrouping(
                SysDefine.FAULT_BOLT_ID,
                SysDefine.FAULT_STREAM,
                new Fields(KafkaStream.BOLT_KEY))
            // es 同步推送
            .fieldsGrouping(
                SysDefine.SYNES_BOLT_ID,
                SysDefine.SYNES_NOTICE,
                new Fields(KafkaStream.BOLT_KEY))
            // 车辆通知
            .fieldsGrouping(
                SysDefine.CUS_NOTICE_BOLT_ID,
                SysDefine.CUS_NOTICE,
                new Fields(KafkaStream.BOLT_KEY))
            // 车辆通知(来自FilterBolt)
            .fieldsGrouping(
                SysDefine.CHECK_FILTER_BOLT_ID,
                SysDefine.CUS_NOTICE,
                new Fields(KafkaStream.BOLT_KEY));
    }

    /**
     * 读取并填充Kafka相关配置
     *
     * @param properties 配置属性
     */
    private static void fillKafkaConf(@NotNull Properties properties) {
        // TODO: 转为存储到单例类

        // Kafka 经纪人及监听的端口, 多个经纪人之间用英文逗号隔开. 从 kafka 0.10.1开始支持新的消费方式
        SysDefine.KAFKA_BOOTSTRAP_SERVERS = properties.getProperty("kafka.bootstrap.servers");
        LOG.info("ConsumerConfig: {}=[{}]", ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SysDefine.KAFKA_BOOTSTRAP_SERVERS);
        LOG.info("KafkaStream.Fields({}, {}, {})", KafkaStream.TOPIC, KafkaStream.BOLT_KEY, KafkaStream.BOLT_MESSAGE);
        LOG.info("ProducerConfig: {}=[{}]", ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SysDefine.KAFKA_BOOTSTRAP_SERVERS);

        // region Spout 输入主题

        // 原始报文 topic, 依赖上游前置机, 请保持一致. 目前约定为 us_packet.
        SysDefine.ERROR_DATA_TOPIC = properties.getProperty("kafka.topic.errordatatopic");
        // 原始报文 consumer-group
        SysDefine.ERROR_DATA_GROUPID = properties.getProperty("kafka.metadata.veh_error_groupid");

        // 车辆实时数据 topic, 依赖上游前置机, 请保持一致. 目前约定为 us_general.
        SysDefine.VEH_REALINFO_DATA_TOPIC = properties.getProperty("kafka.topic.veh_realinfo_data");
        // 车辆实时数据 consumer-group
        SysDefine.VEH_REALINFO_GROUPID = properties.getProperty("kafka.metadata.veh_realinfo_groupid");

        // 车辆注册通知 topic, 依赖上游前置机, 请保持一致. 目前约定为 SYNC_VEHICLE_REG.
        SysDefine.PLAT_REG_TOPIC = properties.getProperty("kafka.platform.veh.reg");
        // 车辆注册通知 consumer-group
        SysDefine.PLAT_REG_GROUPID = properties.getProperty("kafka.platform.group");

        // endregion Spout 输入主题
    }
}

