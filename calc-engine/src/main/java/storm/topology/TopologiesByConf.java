package storm.topology;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.bolt.deal.cusmade.CarNoticeBolt;
import storm.bolt.deal.norm.AlarmBolt;
import storm.bolt.deal.norm.FilterBolt;
import storm.conf.SysDefineEntity;
import storm.constant.StreamFieldKey;
import storm.kafka.bolt.KafkaSendBolt;
import storm.kafka.spout.GeneralKafkaSpout;
import storm.spout.MySqlSpout;
import storm.stream.KafkaStream;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.function.TeConsumer;
import storm.util.function.TeConsumerE;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author xzp
 */
public final class TopologiesByConf {

    private static final Logger LOG = LoggerFactory.getLogger(TopologiesByConf.class);

    /**
     * http://storm.apache.org/releases/current/index.html
     *
     * @param args 拓扑启动参数, 忽略.
     * @throws Exception 拓扑启动异常
     */
    public static void main(@NotNull final String[] args) throws Exception {
        submitTopology(
            args,
            StormSubmitter::submitTopology,
            TopologiesByConf::buildKafkaEmitSpout,
            TopologiesByConf::buildKafkaSendBolt
        );
    }

    private static void buildKafkaEmitSpout(
        @NotNull final TeConsumer<@NotNull String, @NotNull String, @NotNull IRichSpout> buildMessageEmitSpout) {

        buildMessageEmitSpout.accept(
            GeneralKafkaSpout.getComponentId(),
            GeneralKafkaSpout.getGeneralStreamId(),
            new GeneralKafkaSpout(
                ConfigUtils.getSysDefine().getKafkaBootstrapServers(),
                ConfigUtils.getSysDefine().getKafkaConsumerVehicleRealtimeDataTopic(),
                ConfigUtils.getSysDefine().getKafkaConsumerVehicleRealtimeDataGroup()
            )
        );
    }

    private static void buildKafkaSendBolt(
        @NotNull final BiConsumer<@NotNull String, @NotNull IRichBolt> buildMessageSendBolt) {

        buildMessageSendBolt.accept(
            KafkaSendBolt.getComponentId(),
            new KafkaSendBolt(
                ConfigUtils.getSysDefine().getKafkaBootstrapServers()
            )
        );
    }

    public static void submitTopology(
        @NotNull final String[] args,
        @NotNull final TeConsumerE<String, Map, StormTopology, Exception> stormSubmitter,
        @NotNull final Consumer<@NotNull TeConsumer<@NotNull String, @NotNull String, @NotNull IRichSpout>> buildMessageEmitSpout,
        @NotNull final Consumer<@NotNull BiConsumer<@NotNull String, @NotNull IRichBolt>> buildMessageSendBolt)
        throws Exception {

        if (args.length > 0) {
            //args[0] 自定义配置文件名
            File file = new File(args[0]);
            if (!file.exists()) {
                LOG.error("配置文件 {} 不存在", args[0]);
                return;
            }
            if (!file.getName().endsWith(".properties")) {
                LOG.error("配置文件 {} 格式不正确", args[0]);
                return;
            }
            //读取自定义文件
            Properties properties = new Properties();
            ConfigUtils.loadResourceFromLocal(file, properties);
            ConfigUtils.fillSysDefineEntity(properties);
        }
        Config stormConf = buildStormConf();
        StormTopology stormTopology = createTopology(buildMessageEmitSpout, buildMessageSendBolt);
        final String topologyName = ConfigUtils.getSysDefine().getTopologyName();
        if (StringUtils.isEmpty(topologyName)) {
            throw new Exception("topologyName is null");
        }
        stormSubmitter.accept(topologyName, stormConf, stormTopology);
    }

    private static Config buildStormConf() {

        final SysDefineEntity sysDefine = ConfigUtils.getSysDefine();

        final int workerNo = sysDefine.getStormWorkerNo();
        final int workerHeapMemory = sysDefine.getStormWorkerHeapMemoryMb();

        final Config stormConf = readStormConf();
        stormConf.setDebug(false);
        stormConf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);
        stormConf.put(Config.WORKER_HEAP_MEMORY_MB, workerHeapMemory);
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

        //region kafka
        stormConf.put(SysDefine.KAFKA_ZOOKEEPER_SERVERS_KEY, ConfigUtils.getSysDefine().getKafkaZookeeperServers());
        stormConf.put(SysDefine.KAFKA_ZOOKEEPER_PORT_KEY, ConfigUtils.getSysDefine().getKafkaZookeeperPort());
        stormConf.put(SysDefine.KAFKA_ZOOKEEPER_PATH_KEY, ConfigUtils.getSysDefine().getKafkaZookeeperPath());

        stormConf.put(SysDefine.KAFKA_BOOTSTRAP_SERVERS_KEY, ConfigUtils.getSysDefine().getKafkaBootstrapServers());

        stormConf.put(SysDefine.KAFKA_CONSUMER_VEHICLE_REALTIME_DATA_TOPIC, ConfigUtils.getSysDefine().getKafkaConsumerVehicleRealtimeDataTopic());
        stormConf.put(SysDefine.KAFKA_CONSUMER_VEHICLE_REALTIME_DATA_GROUP, ConfigUtils.getSysDefine().getKafkaConsumerVehicleRealtimeDataGroup());

        stormConf.put(SysDefine.VEHICLE_ALARM_TOPIC, ConfigUtils.getSysDefine().getKafkaProducerVehicleAlarmTopic());
        stormConf.put(SysDefine.KAFKA_PRODUCER_VEHICLE_FENCE_ALARM_TOPIC, ConfigUtils.getSysDefine().getKafkaProducerVehicleFenceAlarmTopic());
        stormConf.put(SysDefine.KAFKA_TOPIC_NOTICE, ConfigUtils.getSysDefine().getKafkaProducerVehicleNoticeTopic());

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
    private static StormTopology createTopology(
        @NotNull final Consumer<@NotNull TeConsumer<@NotNull String, @NotNull String, @NotNull IRichSpout>> buildHeadEmitSpout,
        @NotNull final Consumer<@NotNull BiConsumer<@NotNull String, @NotNull IRichBolt>> buildTailSendBolt) {

        final int realSpoutNo = ConfigUtils.getSysDefine().getStormKafkaSpoutNo();
        final int boltNo = ConfigUtils.getSysDefine().getStormKafkaBoltNo();

        TopologyBuilder builder = new TopologyBuilder();

        buildSingleSpout(builder);

        buildHeadEmitSpout.accept((messageEmitSpoutComponentId, messageEmitSpoutStreamId, messageEmitSpout) ->{
            buildMessageEmitSpout(
                builder,
                messageEmitSpoutComponentId,
                messageEmitSpoutStreamId,
                messageEmitSpout,
                realSpoutNo,
                boltNo
            );
        });

        buildMiddleBlots(builder, boltNo);

        buildTailSendBolt.accept((messageSendBoltComponentId, messageSendBolt) ->
            buildMessageSendBolt(
                builder,
                messageSendBoltComponentId,
                messageSendBolt,
                boltNo
            )
        );

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

    private static void buildMessageEmitSpout(
        @NotNull final TopologyBuilder builder,
        @NotNull final String messageEmitSpoutComponentId,
        @NotNull final String messageEmitSpoutStreamId,
        @NotNull final IRichSpout messageEmitSpout,
        final int realSpoutNo,
        final int boltNo) {

        builder.setSpout(
            messageEmitSpoutComponentId,
            messageEmitSpout,
            realSpoutNo
        );

        builder
            .setBolt(
                FilterBolt.getComponentId(),
                new FilterBolt(),
                boltNo)
            .setNumTasks(boltNo * 3)
            // 接收车辆实时数据
            .fieldsGrouping(
                messageEmitSpoutComponentId,
                messageEmitSpoutStreamId,
                new Fields(StreamFieldKey.VEHICLE_ID))
            .fieldsGrouping(
                MySqlSpout.getComponentId(),
                MySqlSpout.getVehicleIdentityStreamId(),
                new Fields(StreamFieldKey.VEHICLE_ID)
            );
    }

    /**
     * @param builder 拓扑构建器
     * @param boltNo  Blot 基准并行度
     */
    @SuppressWarnings("AlibabaMethodTooLong")
    private static void buildMiddleBlots(
        @NotNull final TopologyBuilder builder,
        final int boltNo) {

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
                FilterBolt.getDataCacheStreamId(),
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
    }

    private static void buildMessageSendBolt(
        @NotNull final TopologyBuilder builder,
        @NotNull final String messageSendBoltComponentId,
        @NotNull final IRichBolt messageSendBolt,
        final int boltNo) {

        builder
            // 发送 kafka 消息
            .setBolt(
                messageSendBoltComponentId,
                messageSendBolt,
                boltNo * 2)
            .setNumTasks(boltNo * 6)
            // 车辆平台报警状态、实时需要存储的数据
            .fieldsGrouping(
                AlarmBolt.getComponentId(),
                AlarmBolt.getKafkaStreamId(),
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
}

