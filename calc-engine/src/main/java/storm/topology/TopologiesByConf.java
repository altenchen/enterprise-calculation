package storm.topology;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.bolt.deal.cusmade.CarNoticeBolt;
import storm.bolt.deal.norm.AlarmBolt;
import storm.bolt.deal.norm.EleFenceBolt;
import storm.bolt.deal.norm.FilterBolt;
import storm.bolt.deal.norm.SynEsculBolt;
import storm.constant.StreamFieldKey;
import storm.dao.DataToRedis;
import storm.kafka.bolt.KafkaSendBolt;
import storm.kafka.spout.GeneralKafkaSpout;
import storm.kafka.spout.RegisterKafkaSpout;
import storm.spout.MySqlSpout;
import storm.stream.KafkaStream;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.function.TeConsumerE;

import java.io.File;
import java.util.*;
import java.util.Map;

/**
 * @author xzp
 */
public final class TopologiesByConf {

    private static final Logger LOG = LoggerFactory.getLogger(TopologiesByConf.class);

    /**
     * http://storm.apache.org/releases/current/index.html
     * @param args 拓扑启动参数, 忽略.
     * @throws Exception 拓扑启动异常
     */
    public static void main(String[] args) throws Exception {
        submitTopology(args, StormSubmitter::submitTopology);
    }

    public static void submitTopology(String[] args, @NotNull final TeConsumerE<String, Map, StormTopology, Exception> stormSubmitter)
        throws Exception {
        if( args.length > 0 ){
            //args[0] 自定义配置文件名
            File file = new File(args[0]);
            if( !file.exists() ){
                LOG.error("配置文件 {} 不存在", args[0]);
                return;
            }
            if( !file.getName().endsWith(".properties") ){
                LOG.error("配置文件 {} 格式不正确", args[0]);
                return;
            }
            //读取自定义文件
            Properties properties = new Properties();
            ConfigUtils.loadResourceFromLocal(file, properties);
            ConfigUtils.fillSysDefineEntity(properties);
        }
        Config stormConf = buildStormConf();
        StormTopology stormTopology = createTopology();
        final String topologyName = ConfigUtils.getSysDefine().getTopologyName();
        if( StringUtils.isEmpty(topologyName) ){
            throw new Exception("topologyName is null");
        }
        stormSubmitter.accept(topologyName, stormConf, stormTopology);
    }

    private static Config buildStormConf() {

        final int workerNo = ConfigUtils.getSysDefine().getStormWorkerNo();

        final Config stormConf = readStormConf();
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
     * @return Storm相关配置
     */
    private static Config readStormConf() {

        final Config stormConf = new Config();

        //region alarm
        stormConf.put(SysDefine.ALARM_START_TRIGGER_CONTINUE_COUNT, ConfigUtils.getSysDefine().getAlarmStartTriggerContinueCount());
        stormConf.put(SysDefine.ALARM_START_TRIGGER_TIMEOUT_MILLISECOND, ConfigUtils.getSysDefine().getAlarmStartTriggerTimeoutMillisecond());
        //endregion

        //region ctfo
        stormConf.put("ctfo.cacheDB", ConfigUtils.getSysDefine().getCtfoCacheDB());
        stormConf.put("ctfo.cacheHost", ConfigUtils.getSysDefine().getCtfoCacheHost());
        stormConf.put("ctfo.cachePort", ConfigUtils.getSysDefine().getCtfoCachePort());
        stormConf.put("ctfo.cacheTable", ConfigUtils.getSysDefine().getCtfoCacheTable());
        //endregion

        stormConf.put(SysDefine.DB_CACHE_FLUSH_TIME_SECOND, ConfigUtils.getSysDefine().getDbCacheFlushTime());

        stormConf.put(SysDefine.ES_SEND_TIME, ConfigUtils.getSysDefine().getEsSendTime());

        //region kafka
        stormConf.put(SysDefine.KAFKA_ZOOKEEPER_SERVERS_KEY, ConfigUtils.getSysDefine().getKafkaZookeeperServers());
        stormConf.put(SysDefine.KAFKA_ZOOKEEPER_PORT_KEY, ConfigUtils.getSysDefine().getKafkaZookeeperPort());
        stormConf.put(SysDefine.KAFKA_ZOOKEEPER_PATH_KEY, ConfigUtils.getSysDefine().getKafkaZookeeperPath());

        stormConf.put(SysDefine.KAFKA_BOOTSTRAP_SERVERS_KEY, ConfigUtils.getSysDefine().getKafkaBootstrapServers());

        stormConf.put(SysDefine.KAFKA_CONSUMER_VEHICLE_PACKET_DATA_TOPIC, ConfigUtils.getSysDefine().getKafkaConsumerVehiclePacketDataTopic());
        stormConf.put(SysDefine.KAFKA_CONSUMER_VEHICLE_PACKET_DATA_GROUP, ConfigUtils.getSysDefine().getKafkaConsumerVehiclePacketDataGroup());
        stormConf.put(SysDefine.KAFKA_CONSUMER_VEHICLE_REALTIME_DATA_TOPIC, ConfigUtils.getSysDefine().getKafkaConsumerVehicleRealtimeDataTopic());
        stormConf.put(SysDefine.KAFKA_CONSUMER_VEHICLE_REALTIME_DATA_GROUP, ConfigUtils.getSysDefine().getKafkaConsumerVehicleRealtimeDataGroup());
        stormConf.put(SysDefine.KAFKA_CONSUMER_VEHICLE_REGISTER_DATA_TOPIC, ConfigUtils.getSysDefine().getKafkaConsumerVehicleRegisterDataTopic());
        stormConf.put(SysDefine.KAFKA_CONSUMER_VEHICLE_REGISTER_DATA_GROUP, ConfigUtils.getSysDefine().getKafkaConsumerVehicleRegisterDataGroup());

        stormConf.put(SysDefine.VEHICLE_ALARM_TOPIC, ConfigUtils.getSysDefine().getKafkaProducerVehicleAlarmTopic());
        stormConf.put(SysDefine.VEHICLE_ALARM_STORE_TOPIC, ConfigUtils.getSysDefine().getKafkaProducerVehicleAlarmStoreTopic());
        stormConf.put(SysDefine.KAFKA_PRODUCER_VEHICLE_FENCE_ALARM_TOPIC, ConfigUtils.getSysDefine().getKafkaProducerVehicleFenceAlarmTopic());
        stormConf.put(SysDefine.KAFKA_TOPIC_NOTICE, ConfigUtils.getSysDefine().getKafkaProducerVehicleNoticeTopic());
        stormConf.put(SysDefine.KAFKA_TOPIC_ES_STATUS, ConfigUtils.getSysDefine().getKafkaProducerElasticSearchStatusTopic());

        stormConf.put("offline.check.time", ConfigUtils.getSysDefine().getOfflineCheckTime());

        stormConf.put("redis.cluster.data.syn", ConfigUtils.getSysDefine().getRedisClusterDataSyn());
        stormConf.put("redis.host", ConfigUtils.getSysDefine().getRedisHost());
        stormConf.put("redis.listenInterval", ConfigUtils.getSysDefine().getRedisListenInterval());
        stormConf.put("redis.maxActive", ConfigUtils.getSysDefine().getRedisMaxActive());
        stormConf.put("redis.maxIdle", ConfigUtils.getSysDefine().getRedisMaxIdle());
        stormConf.put("redis.maxWait", ConfigUtils.getSysDefine().getRedisMaxWait());
        stormConf.put("redis.pass", ConfigUtils.getSysDefine().getRedisPass());
        stormConf.put("redis.port", ConfigUtils.getSysDefine().getRedisPort());
        stormConf.put("redis.timeInterval", ConfigUtils.getSysDefine().getRedisTimeInterval());
        stormConf.put("redis.timeOut", ConfigUtils.getSysDefine().getRedisTimeOut());
        stormConf.put("redis.offline.checktime", ConfigUtils.getSysDefine().getRedisOfflineCheckTime());
        stormConf.put(StormConfigKey.REDIS_OFFLINE_SECOND, ConfigUtils.getSysDefine().getRedisOfflineTime());

        stormConf.put("storm.kafka.spout.no", ConfigUtils.getSysDefine().getStormKafkaSpoutNo());
        stormConf.put("storm.worker.bolt.no", ConfigUtils.getSysDefine().getStormKafkaBoltNo());
        stormConf.put("storm.worker.no", ConfigUtils.getSysDefine().getStormWorkerNo());

        stormConf.put(SysDefine.NOTICE_CAN_FAULT_TRIGGER_CONTINUE_COUNT, ConfigUtils.getSysDefine().getNoticeCanFaultTriggerContinueCount());
        stormConf.put(SysDefine.NOTICE_CAN_FAULT_TRIGGER_TIMEOUT_MILLISECOND, ConfigUtils.getSysDefine().getNoticeCanFaultTriggerTimeoutMillisecond());
        stormConf.put(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_CONTINUE_COUNT, ConfigUtils.getSysDefine().getNoticeCanNormalTriggerContinueCount());
        stormConf.put(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND, ConfigUtils.getSysDefine().getNoticeCanNormalTriggerTimeoutMillisecond());

        stormConf.put(SysDefine.RULE_OVERRIDE, ConfigUtils.getSysDefine().getRuleOverride());

        return stormConf;
    }

    /**
     * 创建 Storm 拓扑
     *
     * @return Storm 拓扑
     */
    private static StormTopology createTopology() {

        final int realSpoutNo = ConfigUtils.getSysDefine().getStormKafkaSpoutNo();
        final int boltNo = ConfigUtils.getSysDefine().getStormKafkaBoltNo();

        TopologyBuilder builder = new TopologyBuilder();

        buildSingleSpout(builder);

        buildKafkaSpout(builder, realSpoutNo);

        buildBlots(builder, boltNo);

        return builder.createTopology();
    }

    private static void buildSingleSpout(@NotNull final TopologyBuilder builder) {

        builder
            .setSpout(
                MySqlSpout.getComponentId(),
                new MySqlSpout(),
                1
            );
    }

    /**
     * @param builder     拓扑构建器
     * @param realSpoutNo Spout 基准并行度
     */
    private static void buildKafkaSpout(@NotNull final TopologyBuilder builder, final int realSpoutNo) {

        // kafka 实时报文消息
        final KafkaSpout<String, String> generalKafkaSpout = new GeneralKafkaSpout(
                ConfigUtils.getSysDefine().getKafkaBootstrapServers(),
            ConfigUtils.getSysDefine().getKafkaConsumerVehicleRealtimeDataTopic(),
            ConfigUtils.getSysDefine().getKafkaConsumerVehicleRealtimeDataGroup()
        );
        builder.setSpout(
            GeneralKafkaSpout.getComponentId(),
            generalKafkaSpout,
            realSpoutNo
        );

        // kafka 平台注册报文消息
        final KafkaSpout<String, String> registerKafkaSpout = new RegisterKafkaSpout(
                ConfigUtils.getSysDefine().getKafkaBootstrapServers(),
                ConfigUtils.getSysDefine().getKafkaConsumerVehicleRegisterDataTopic(),
                ConfigUtils.getSysDefine().getKafkaConsumerVehicleRegisterDataGroup()
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
    private static void buildBlots(@NotNull final TopologyBuilder builder, final int boltNo) {

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
                new Fields(StreamFieldKey.VEHICLE_ID))
            .fieldsGrouping(
                MySqlSpout.getComponentId(),
                MySqlSpout.getVehicleIdentityStreamId(),
                new Fields(StreamFieldKey.VEHICLE_ID)
            );

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
                CarNoticeBolt.getComponentId(),
                new CarNoticeBolt(),
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

        final KafkaBolt<String, String> kafkaBolt = new KafkaSendBolt(ConfigUtils.getSysDefine().getKafkaBootstrapServers());

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
                CarNoticeBolt.getComponentId(),
                CarNoticeBolt.getKafkaStreamId(),
                new Fields(KafkaStream.BOLT_KEY))
            // 车辆通知
            .fieldsGrouping(
                FilterBolt.getComponentId(),
                FilterBolt.getKafkaStreamId(),
                new Fields(KafkaStream.BOLT_KEY));
    }

    /**
     * 读取并填充Kafka相关配置
     */
    private static void fillKafkaConf() {
        // TODO: 转为存储到单例类

        // Kafka 依赖的 Zookeeper 集群, 为了兼容旧版 kafka
        initZookeeperConfig();

        // Kafka 经纪人及监听的端口, 多个经纪人之间用英文逗号隔开. 从 kafka 0.10.1开始支持新的消费方式
        LOG.info("ConsumerConfig: {}=[{}]", ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.getSysDefine().getKafkaBootstrapServers());
        LOG.info("KafkaStream.Fields({}, {}, {})", KafkaStream.TOPIC, KafkaStream.BOLT_KEY, KafkaStream.BOLT_MESSAGE);
        LOG.info("ProducerConfig: {}=[{}]", ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.getSysDefine().getKafkaBootstrapServers());
    }

    public static void initZookeeperConfig() {

        List<String> servers = Arrays.asList(
                StringUtils.split(
                        ConfigUtils.getSysDefine().getKafkaZookeeperServers(),
                        ','));

        StringBuilder zkServersBuilder = new StringBuilder(64);
        zkServersBuilder.append(servers.get(0));
        zkServersBuilder.append(':');
        zkServersBuilder.append(ConfigUtils.getSysDefine().getKafkaZookeeperPort());
        for (int i = 1; i < servers.size(); ++i) {
            zkServersBuilder.append(',');
            zkServersBuilder.append(servers.get(i));
            zkServersBuilder.append(':');
            zkServersBuilder.append(ConfigUtils.getSysDefine().getKafkaZookeeperPort());
        }
    }
}

