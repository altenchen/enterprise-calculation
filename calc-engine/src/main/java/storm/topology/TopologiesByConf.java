package storm.topology;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.Scheme;
import org.jetbrains.annotations.NotNull;
import storm.bolt.deal.KafkaSendBolt;
import storm.bolt.deal.cusmade.CarNoticelBolt;
import storm.bolt.deal.cusmade.UserActionBolt;
import storm.bolt.deal.norm.AlarmBolt;
import storm.bolt.deal.norm.EleFenceBolt;
import storm.bolt.deal.norm.FaultBolt;
import storm.bolt.deal.norm.FilterBolt;
import storm.bolt.deal.norm.SynEsculBolt;

import java.util.Properties;

import storm.kafka.scheme.PacketScheme;
import storm.kafka.scheme.RegistScheme;
import storm.stream.CusNoticeGroupStream;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.util.ConfigUtils;
import storm.kafka.scheme.GeneralScheme;
import storm.kafka.KafkaConfig;
import storm.system.SysDefine;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologiesByConf {

    private static final ConfigUtils configUtils = ConfigUtils.getInstance();

    public static void main(String[] args) throws Exception{

        Properties properties = configUtils.sysDefine;

        fillKafkaConf(properties);

        Config stormConf = buildStormConf(properties);
        StormTopology stormTopology = createTopology(properties);
        final String topologyName = properties.getProperty(SysDefine.TOPOLOGY_NAME, "qyallStorm");
        StormSubmitter.submitTopology(topologyName, stormConf, stormTopology);

//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                RedisTotalCacheInitUtil.init();
//            }
//        });
    }

    private static Config buildStormConf(@NotNull Properties properties) {

        final int workerNo=Integer.valueOf(properties.getProperty("storm.worker.no"));

        final Config stormConf = readStormConf(properties);
        stormConf.setDebug(false);
        stormConf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);
        stormConf.setMaxSpoutPending(1000);
        stormConf.setNumWorkers(workerNo);
        //设置不需要应答
        stormConf.setNumAckers(0);

        return stormConf;
    }

    /**
     * 读取Storm相关配置
     * @param properties 配置属性
     * @return Storm相关配置
     */
    private static Config readStormConf(@NotNull Properties properties) {

        final Config stormConf = new Config();

        //region alarm
        // 连续多少条报警才发送通知
        stormConf.put("alarm.continue.counts", properties.get("alarm.continue.counts"));
        //endregion

        //region ctfo
        stormConf.put("ctfo.cacheDB", properties.getProperty("ctfo.cacheDB"));
        stormConf.put("ctfo.cacheHost", properties.getProperty("ctfo.cacheHost"));
        stormConf.put("ctfo.cachePort", properties.getProperty("ctfo.cachePort"));
        stormConf.put("ctfo.cacheTable", properties.getProperty("ctfo.cacheTable"));
        //endregion

        stormConf.put(SysDefine.DB_CACHE_FLUSH_TIME_SECOND, properties.get(SysDefine.DB_CACHE_FLUSH_TIME_SECOND));

        stormConf.put("es.send.time", properties.get("es.send.time"));

        //region kafka
        stormConf.put("kafka.customer.hosts", properties.getProperty("kafka.broker.hosts"));
        stormConf.put("kafka.platform.veh.reg", properties.get("kafka.platform.veh.reg"));
        stormConf.put("kafka.topic.action", properties.get("kafka.topic.action"));
        stormConf.put(SysDefine.KAFKA_TOPIC_ALARM, properties.getProperty(SysDefine.KAFKA_TOPIC_ALARM));
        stormConf.put("kafka.topic.alarmstore", properties.getProperty("kafka.topic.alarmstore"));
        stormConf.put("kafka.topic.customfault", properties.get("kafka.topic.customfault"));
        stormConf.put("kafka.topic.es.status", properties.get("kafka.topic.es.status"));
        stormConf.put("kafka.topic.fencealarm", properties.get("kafka.topic.fencealarm"));
        stormConf.put("kafka.topic.notice", properties.get("kafka.topic.notice"));
        stormConf.put("kafka.topic.realinfostore", properties.getProperty("kafka.topic.realinfostore"));
        //endregion

        stormConf.put("offline.check.time", properties.get("offline.check.time"));

        stormConf.put("producer.againNo", properties.get("producer.againNo"));
        stormConf.put("producer.poolNo", properties.get("producer.poolNo"));

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

        // region notice.time
        stormConf.put(SysDefine.NOTICE_TIME_RANGE_ABS_MILLISECOND, properties.getProperty(SysDefine.NOTICE_TIME_RANGE_ABS_MILLISECOND));
        // endregion

        // region jili
        stormConf.put(SysDefine.RULE_OVERRIDE, properties.getProperty(SysDefine.RULE_OVERRIDE));
        // endregion

        return stormConf;
    }

    /**
     * 创建 Storm 拓扑
     * @param properties 配置属性
     * @return Storm 拓扑
     */
    private static StormTopology createTopology(@NotNull Properties properties) {

        final int realSpoutNo=Integer.valueOf(properties.getProperty("storm.kafka.spout.no"));
        final int boltNo=Integer.valueOf(properties.getProperty("storm.worker.bolt.no"));

        TopologyBuilder builder = new TopologyBuilder();

        buildKafkaSpout(builder, realSpoutNo);

        builderBlots(builder, boltNo);

        return builder.createTopology();
    }

    /**
     * @param builder 拓扑构建器
     * @param realSpoutNo Spout 基准并行度
     */
    private static void buildKafkaSpout(@NotNull TopologyBuilder builder, int realSpoutNo) {

        // KafkaSpout: 实时数据
        final KafkaConfig kafkaRealinfoConfig = buildKafkaConfig(
            SysDefine.VEH_REALINFO_DATA_TOPIC,
            SysDefine.VEH_REALINFO_GROUPID,
            new GeneralScheme());
        builder
            // kafka实时报文消息
            .setSpout(
                SysDefine.REALINFO_SPOUT_ID,
                new KafkaSpout(kafkaRealinfoConfig.getSpoutConfig()),
                realSpoutNo);

        // KafkaSpout: 错误报文
        final KafkaConfig kafkaErrordataConfig = buildKafkaConfig(
            SysDefine.ERROR_DATA_TOPIC,
            SysDefine.ERROR_DATA_GROUPID,
            new PacketScheme());
        builder
            // kafka错误报文消息
            .setSpout(
                SysDefine.ERRORDATA_SPOUT_ID,
                new KafkaSpout(kafkaErrordataConfig.getSpoutConfig()),
                realSpoutNo);

        // KafkaSpout: 平台注册报文
        final KafkaConfig kafkaRegConfig = buildKafkaConfig(
                SysDefine.PLAT_REG_TOPIC,
                SysDefine.PLAT_REG_GROUPID,
                new RegistScheme());
        kafkaRegConfig.setOutputStreamId(SysDefine.REG_STREAM_ID);
        builder
                // kafka平台注册报文消息
                .setSpout(
                        SysDefine.REG_SPOUT_ID,
                        new KafkaSpout(kafkaRegConfig.getSpoutConfig()),
                        realSpoutNo);

    }

    /**
     * @param builder 拓扑构建器
     * @param boltNo Blot 基准并行度
     */
    private static void builderBlots(@NotNull TopologyBuilder builder, int boltNo){

        builder
            .setBolt(
                SysDefine.CHECK_FILTER_BOLT_ID,
                new FilterBolt(),
                boltNo)
            .setNumTasks(boltNo * 3)
            // 接收车辆错误报文数据
            .fieldsGrouping(
                SysDefine.ERRORDATA_SPOUT_ID,
                new Fields(DataKey.VEHICLE_ID))
            // 接收车辆实时数据
            .fieldsGrouping(
                SysDefine.REALINFO_SPOUT_ID,
                new Fields(DataKey.VEHICLE_ID));

        builder
            // 预警处理
            .setBolt(
                SysDefine.ALARM_BOLT_ID,
                new AlarmBolt(),
                boltNo*3)
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

        final CusNoticeGroupStream cusNoticeGroupStream = new CusNoticeGroupStream();
        builder
            // soc 与超时处理
            .setBolt(
                SysDefine.CUS_NOTICE_BOLT_ID,
                new CarNoticelBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
            // soc 与超时处理实时数据
            .fieldsGrouping(
                SysDefine.CHECK_FILTER_BOLT_ID,
                cusNoticeGroupStream.getStreamId(),
                new Fields(DataKey.VEHICLE_ID));

        builder
            // 雅安用户行为处理
            .setBolt(
                SysDefine.YAACTION_BOLT_ID,
                new UserActionBolt(),
                boltNo * 3)
            .setNumTasks(boltNo*9)
            // 雅安用户行为实时数据
            .fieldsGrouping(
                SysDefine.CHECK_FILTER_BOLT_ID,
                SysDefine.YAACTION_GROUP,
                new Fields(DataKey.VEHICLE_ID));

        builder
            // es数据同步处理
            .setBolt(
                SysDefine.SYNES_BOLT_ID,
                new SynEsculBolt(),
                boltNo * 3)
            .setNumTasks(boltNo * 9)
//            // 预警信息通知同步数据
//            .fieldsGrouping(
//                SysDefine.ALARM_BOLT_ID,
//                SysDefine.SYNES_GROUP,
//                new Fields(SysDefine.VEHICLE_ID))
            // 电子围栏告警实时数据
            .fieldsGrouping(
                SysDefine.CHECK_FILTER_BOLT_ID,
                SysDefine.SYNES_GROUP,
                new Fields(DataKey.VEHICLE_ID))
            .noneGrouping(
                SysDefine.REG_SPOUT_ID,
                SysDefine.REG_STREAM_ID);

//        builder
//            // 历史补发相关计算
//            .setBolt(
//                SysDefine.QUICK_BOLT_ID,
//                new QuickCacheBolt(),
//                boltNo * 2)
//            .setNumTasks(boltNo * 6)
//            .fieldsGrouping(
//                SysDefine.CHECK_FILTER_BOLT_ID,
//                SysDefine.SUPPLY_GROUP,
//                new Fields(SysDefine.VEHICLE_ID));

        builder
            // 发送kafka消息，必要时可以动态增加线程数以增加发送线程数据
            .setBolt(
                SysDefine.KAFKASEND_BOLT_ID,
                new KafkaSendBolt(),
                boltNo * 2)
            .setNumTasks(boltNo * 6)
            // 车辆告警数据
            .fieldsGrouping(
                SysDefine.ALARM_BOLT_ID,
                SysDefine.VEH_ALARM,
                new Fields(DataKey.VEHICLE_ID))
            // 车辆报警状态、实时需要存储的数据
            .fieldsGrouping(
                SysDefine.ALARM_BOLT_ID,
                SysDefine.VEH_ALARM_REALINFO_STORE,
                new Fields(DataKey.VEHICLE_ID))
            // 电子围栏
            .fieldsGrouping(
                SysDefine.FENCE_BOLT_ID,
                SysDefine.FENCE_ALARM,
                new Fields(DataKey.VEHICLE_ID))
            // 故障处理
            .fieldsGrouping(
                SysDefine.FAULT_BOLT_ID,
                SysDefine.FAULT_STREAM,
                new Fields(DataKey.VEHICLE_ID))
            // 雅安公交驾驶行为
            .fieldsGrouping(
                SysDefine.YAACTION_BOLT_ID,
                SysDefine.YAACTION_NOTICE,
                new Fields(DataKey.VEHICLE_ID))
            // es 同步推送
            .fieldsGrouping(
                SysDefine.SYNES_BOLT_ID,
                SysDefine.SYNES_NOTICE,
                new Fields(DataKey.VEHICLE_ID))
            // 告警 同步推送
            .fieldsGrouping(
                SysDefine.CUS_NOTICE_BOLT_ID,
                SysDefine.CUS_NOTICE,
                new Fields(DataKey.VEHICLE_ID))
            // 历史数据直接存储
            .fieldsGrouping(
                SysDefine.CHECK_FILTER_BOLT_ID,
                SysDefine.HISTORY,
                new Fields(DataKey.VEHICLE_ID));
    }

    /**
     * 构建kafkaSpout配置
     * @param topic kafka主题
     * @return kafka配置
     */
    private static KafkaConfig buildKafkaConfig(String topic, String spoutId, Scheme scheme) {
        KafkaConfig kafkaConfig = new KafkaConfig(
                topic,
                SysDefine.KAFKA_ZK_ROOT,
                spoutId,
                SysDefine.KAFKA_ZK_HOSTS,
                scheme);
        kafkaConfig.setZKConfig(
                SysDefine.KAFKA_ZK_PORT,
                SysDefine.KAFKA_ZK_SERVERS.split(","));
        return kafkaConfig;
    }

    /**
     * 读取并填充Kafka相关配置
     * @param properties 配置属性
     */
    private static void fillKafkaConf(@NotNull Properties properties){
        // TODO: 转为存储到单例类

        // Zookeeper 主机
        SysDefine.KAFKA_ZK_SERVERS = properties.getProperty("kafka.zkServers");
        // Zookeeper 端口
        SysDefine.KAFKA_ZK_PORT = Integer.valueOf(properties.getProperty("kafka.zkPort"));
        // Zookeeper 地址
        SysDefine.KAFKA_ZK_HOSTS = properties.getProperty("kafka.zk.hosts");
        // Zookeeper 根目录
        SysDefine.KAFKA_ZK_ROOT = properties.getProperty("kafka.zkroot");

        // 从 kafka 0.10.1开始支持新的消费方式
        SysDefine.KAFKA_BOOTSTRAP_SERVERS = properties.getProperty("kafka.bootstrap.servers");

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

