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
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.bolt.deal.cusmade.CarNoticelBolt;
import storm.bolt.deal.norm.AlarmBolt;
import storm.bolt.deal.norm.EleFenceBolt;
import storm.bolt.deal.norm.FilterBolt;
import storm.bolt.deal.norm.SynEsculBolt;
import storm.constant.StreamFieldKey;
import storm.kafka.bolt.KafkaSendBolt;
import storm.kafka.spout.GeneralKafkaSpout;
import storm.kafka.spout.RegisterKafkaSpout;
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
        stormConf.put(StormConfigKey.REDIS_OFFLINE_SECOND, properties.get(StormConfigKey.REDIS_OFFLINE_SECOND));
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
        final KafkaSpout<String, String> generalKafkaSpout = new GeneralKafkaSpout(
            SysDefine.KAFKA_BOOTSTRAP_SERVERS,
            SysDefine.VEH_REALINFO_DATA_TOPIC,
            SysDefine.VEH_REALINFO_GROUPID
        );
        builder.setSpout(
            GeneralKafkaSpout.getComponentId(),
            generalKafkaSpout,
            realSpoutNo
        );

        // kafka 平台注册报文消息
        final KafkaSpout<String, String> registerKafkaSpout = new RegisterKafkaSpout(
            SysDefine.KAFKA_BOOTSTRAP_SERVERS,
            SysDefine.PLAT_REG_TOPIC,
            SysDefine.PLAT_REG_GROUPID
        );
        builder.setSpout(
            RegisterKafkaSpout.getComponentId(),
            registerKafkaSpout,
            realSpoutNo
        );
    }

    /**
     * @param builder 拓扑构建器
     * @param boltNo  Blot 基准并行度
     */
    @SuppressWarnings("AlibabaMethodTooLong")
    private static void builderBlots(@NotNull final TopologyBuilder builder, final int boltNo) {

        builder
            .setBolt(
                FilterBolt.getComponentId(),
                new FilterBolt(),
                boltNo)
            .setNumTasks(boltNo * 3)
            // 接收车辆实时数据
            .fieldsGrouping(
                GeneralKafkaSpout.getComponentId(),
                GeneralKafkaSpout.getGeneralStreamId(),
                new Fields(StreamFieldKey.VEHICLE_ID));

        builder
            // 预警处理
            .setBolt(
                AlarmBolt.getComponentId(),
                new AlarmBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
            // 预警的车辆实时数据
            .fieldsGrouping(
                FilterBolt.getComponentId(),
                SysDefine.SPLIT_GROUP,
                new Fields(DataKey.VEHICLE_ID));

        builder
            // 电子围栏告警处理
            .setBolt(
                EleFenceBolt.getComponentId(),
                new EleFenceBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
            // 电子围栏告警实时数据
            .fieldsGrouping(
                FilterBolt.getComponentId(),
                SysDefine.FENCE_GROUP,
                new Fields(DataKey.VEHICLE_ID));

        builder
            // 通知处理、故障码处理
            .setBolt(
                CarNoticelBolt.getComponentId(),
                new CarNoticelBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
            // soc 与超时处理实时数据
            .fieldsGrouping(
                FilterBolt.getComponentId(),
                FilterBolt.getDataStreamId(),
                new Fields(DataKey.VEHICLE_ID));

        builder
            // es数据同步处理
            .setBolt(
                SynEsculBolt.getComponentId(),
                new SynEsculBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
            .fieldsGrouping(
                FilterBolt.getComponentId(),
                SysDefine.SYNES_GROUP,
                new Fields(DataKey.VEHICLE_ID))
            .noneGrouping(
                RegisterKafkaSpout.getComponentId(),
                RegisterKafkaSpout.getRegisterStreamId());

        buildKafkaBolt(builder, boltNo);
    }

    private static void buildKafkaBolt(
        @NotNull final TopologyBuilder builder,
        final int boltNo) {

        final KafkaBolt<String, String> kafkaBolt = new KafkaSendBolt(SysDefine.KAFKA_BOOTSTRAP_SERVERS);

        builder
            // 发送 kafka 消息
            .setBolt(
                KafkaSendBolt.getComponentId(),
                kafkaBolt,
                boltNo * 2)
            .setNumTasks(boltNo * 6)
            // 车辆平台报警状态、实时需要存储的数据
            .fieldsGrouping(
                AlarmBolt.getComponentId(),
                AlarmBolt.getKafkaStreamId(),
                new Fields(KafkaStream.BOLT_KEY))
            // 电子围栏
            .fieldsGrouping(
                EleFenceBolt.getComponentId(),
                EleFenceBolt.getKafkaStreamId(),
                new Fields(KafkaStream.BOLT_KEY))
            // es 同步推送
            .fieldsGrouping(
                SynEsculBolt.getComponentId(),
                SynEsculBolt.getKafkaStreamId(),
                new Fields(KafkaStream.BOLT_KEY))
            // 车辆通知、故障处理
            .fieldsGrouping(
                CarNoticelBolt.getComponentId(),
                CarNoticelBolt.getKafkaStreamId(),
                new Fields(KafkaStream.BOLT_KEY))
            // 车辆通知
            .fieldsGrouping(
                FilterBolt.getComponentId(),
                FilterBolt.getKafkaStreamId(),
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

