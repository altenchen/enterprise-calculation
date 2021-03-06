package storm.bolt.deal.cusmade;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.bolt.deal.norm.FilterBolt;
import storm.cache.SysRealDataCache;
import storm.cache.VehicleCache;
import storm.dao.DataToRedis;
import storm.handler.FaultCodeHandler;
import storm.handler.cusmade.CarOnOffHandler;
import storm.handler.cusmade.CarOnOffMileJudge;
import storm.handler.cusmade.CarRuleHandler;
import storm.protocol.CommandType;
import storm.stream.KafkaStream;
import storm.stream.StreamReceiverFilter;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author xzp
 */
public final class CarNoticeBolt extends BaseRichBolt {

    private static final long serialVersionUID = -1010194368397854277L;

    private static final Logger LOG = LoggerFactory.getLogger(CarNoticeBolt.class);

    // region Component

    @NotNull
    private static final String COMPONENT_ID = CarNoticeBolt.class.getSimpleName();

    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    // region KafkaStream

    @NotNull
    private static final KafkaStream KAFKA_STREAM = KafkaStream.getInstance();

    @NotNull
    private static final String KAFKA_STREAM_ID = KAFKA_STREAM.getStreamId(COMPONENT_ID);

    @NotNull
    @Contract(pure = true)
    public static String getKafkaStreamId() {
        return KAFKA_STREAM_ID;
    }

    // endregion KafkaStream

    private static final DataToRedis redis = new DataToRedis();

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    private transient OutputCollector collector;

    private transient StreamReceiverFilter dataStreamReceiver;

    private transient KafkaStream.SenderBuilder kafkaStreamSenderBuilder;

    private transient KafkaStream.Sender kafkaStreamVehicleNoticeSender;

    /**
     * 最后进行离线检查的时间, 用于离线判断
     */
    private transient long lastOfflineCheckTimeMillisecond;

    /**
     * 车辆规则处理
     */
    private transient CarRuleHandler carRuleHandler;

    /**
     * 车辆上下线及相关处理
     */
    private transient CarOnOffHandler carOnOffhandler;

    /**
     * 车辆上下线里程通知处理
     */
    private transient CarOnOffMileJudge carOnOffMileJudge;

    /**
     * 故障码处理
     */
    private transient FaultCodeHandler faultCodeHandler;

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {
        //将storm启动时的自定义参数设置进来
        LOG.info("将STORM启动时设置的参数填充进来");
        ConfigUtils.fillSysDefineEntity(stormConf);
        //首次从redis读取配置
        LOG.info("将REDIS动态设置的参数填充进来");
        ConfigUtils.readConfigFromRedis(redis);

        this.collector = collector;

        carRuleHandler = new CarRuleHandler();
        carOnOffhandler = new CarOnOffHandler();
        faultCodeHandler = new FaultCodeHandler();
        carOnOffMileJudge = new CarOnOffMileJudge();

        prepareStreamSender(stormConf, collector);

        prepareStreamReceiver();

        prepareIdleVehicleThread();
    }

    private void prepareStreamSender(
        @NotNull final Map stormConf,
        @NotNull final OutputCollector collector) {

        kafkaStreamSenderBuilder = KAFKA_STREAM.prepareSender(KAFKA_STREAM_ID, collector);

        // 输出到Kafka的主题
        final String noticeTopic = stormConf.get(SysDefine.KAFKA_TOPIC_NOTICE).toString();

        kafkaStreamVehicleNoticeSender = kafkaStreamSenderBuilder.build(noticeTopic);

        faultCodeHandler.setKafkaStreamVehicleNoticeSender(kafkaStreamVehicleNoticeSender);
    }

    private void prepareStreamReceiver() {

        dataStreamReceiver = FilterBolt.prepareDataStreamReceiver(this::executeFromDataStream);
    }

    @SuppressWarnings("AlibabaMethodTooLong")
    private void prepareIdleVehicleThread() {

        final long currentTimeMillis = System.currentTimeMillis();
        lastOfflineCheckTimeMillisecond = currentTimeMillis;

        SysRealDataCache.init();
    }

    @Override
    public void execute(
        @NotNull final Tuple input) {

        if(TupleUtils.isTick(input)) {
            executeFromSystemTickStream(input);
            return;
        }

        if (dataStreamReceiver.execute(input)) {
            return;
        }
    }

    /**
     * Bolt 时钟, 当前配置为每分钟执行一次.
     * @param input
     */
    private void executeFromSystemTickStream(
        @NotNull final Tuple input) {

        collector.ack(input);

        ConfigUtils.readConfigFromRedis(redis);

        final long currentTimeMillis = System.currentTimeMillis();

        final long offlineCheckSpanMillisecond = ConfigUtils.getSysDefine().getRedisOfflineCheckTime() * 1000;
        // region 离线判断: 如果时间差大于离线检查时间，则进行离线检查
        if (currentTimeMillis - lastOfflineCheckTimeMillisecond >= offlineCheckSpanMillisecond) {

            lastOfflineCheckTimeMillisecond = currentTimeMillis;

            // 如果车辆离线，则发送此车辆的所有故障码结束通知
            {
                final List<Map<String, Object>> notices = faultCodeHandler.generateNotice(currentTimeMillis);
                if (CollectionUtils.isNotEmpty(notices)) {
                    for (final Map<String, Object> notice : notices) {
                        if (MapUtils.isNotEmpty(notice)) {
                            String json = JSON_UTILS.toJson(notice);
                            final String vid = ObjectUtils.toString(notice.get("vid"), null);
                            if (null != vid) {
                                kafkaStreamVehicleNoticeSender.emit(vid, json);
                            } else {
                                LOG.warn("不含vid的故障码通知[{}]", json);
                            }
                        }
                    }
                }
            }

            //检查所有车辆是否离线，离线则发送离线通知。
            {
                final List<Map<String, Object>> notices = carRuleHandler.offlineMethod(currentTimeMillis);
                if (CollectionUtils.isNotEmpty(notices)) {
                    for (final Map<String, Object> notice : notices) {
                        if (MapUtils.isNotEmpty(notice)) {
                            String json = JSON_UTILS.toJson(notice);
                            final String vid = ObjectUtils.toString(notice.get("vid"), null);
                            if (null != vid) {
                                kafkaStreamVehicleNoticeSender.emit(vid, json);
                            } else {
                                LOG.warn("不含vid的车辆离线通知[{}]", json);
                            }
                        }
                    }
                }
            }

            long offlineTimeMillisecond = ConfigUtils.getSysDefine().getRedisOfflineTime() * 1000;
            carOnOffhandler.onOffCheck("TIMEOUT", 1, currentTimeMillis, offlineTimeMillisecond);
        }
        // endregion
    }

    @SuppressWarnings("AlibabaMethodTooLong")
    private void executeFromDataStream(
        @NotNull final Tuple input,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data) {
        collector.ack(input);
        try{

            final long currentTimeMillis = System.currentTimeMillis();

            if (MapUtils.isEmpty(data)) {
                return;
            }

            LOG.warn("VID:{} 进入车辆通知处理", vehicleId);

            // region 缓存有效状态

            VEHICLE_CACHE.updateUsefulCache(data);

            // endregion 缓存有效状态

            // region 更新实时缓存
            try {
                final String type = data.get(DataKey.MESSAGE_TYPE);
                if (!CommandType.SUBMIT_LINKSTATUS.equals(type)) {
                    long idleTimeoutMillisecond = ConfigUtils.getSysDefine().getVehicleIdleTimeoutMillisecond() * 1000;
                    SysRealDataCache.updateCache(data, currentTimeMillis, idleTimeoutMillisecond);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            // endregion

            //返回车辆通知,(重要)
            //先检查规则是否启用，启用了，则把dat放到相应的处理方法中。将返回结果放到list中，返回。
            final ImmutableList<String> jsonNotices = carRuleHandler.generateNotices(vehicleId, data);
            for (final String json : jsonNotices) {
                kafkaStreamVehicleNoticeSender.emit(vehicleId, json);
            }

            final List<Map<String, Object>> faultCodeMessages = faultCodeHandler.generateNotice(data);
            if (CollectionUtils.isNotEmpty(faultCodeMessages)) {
                for (Map<String, Object> map : faultCodeMessages) {
                    if (null != map && map.size() > 0) {
                        String json = JSON_UTILS.toJson(map);
                        kafkaStreamVehicleNoticeSender.emit(vehicleId, json);
                    }
                }
            }

            //当车辆上线时发出上下线里程通知，具体是否产生再次上线里程跳变告警，是由kafkaservice做判断的。
            String onOffMileNotice = carOnOffMileJudge.processFrame(data);
            if (StringUtils.isNotEmpty(onOffMileNotice)) {
                kafkaStreamVehicleNoticeSender.emit(vehicleId, onOffMileNotice);
            }
        }catch (Exception e){
            LOG.error("VID:" + vehicleId + " 异常", e);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {

        KAFKA_STREAM.declareOutputFields(KAFKA_STREAM_ID, declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        final Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TimeUnit.MINUTES.toSeconds(1));
        return config;
    }
}
