package storm.bolt.deal.cusmade;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
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
import storm.handler.FaultCodeHandler;
import storm.handler.cusmade.CarOnOffHandler;
import storm.handler.cusmade.CarRuleHandler;
import storm.handler.cusmade.ScanRange;
import storm.protocol.CommandType;
import storm.stream.KafkaStream;
import storm.stream.StreamReceiverFilter;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.DataUtils;
import storm.util.JsonUtils;
import storm.util.ParamsRedisUtil;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
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

    private static final ParamsRedisUtil PARAMS_REDIS_UTIL = ParamsRedisUtil.getInstance();

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    private OutputCollector collector;

    private StreamReceiverFilter dataStreamReceiver;

    private KafkaStream.SenderBuilder kafkaStreamSenderBuilder;

    private KafkaStream.Sender kafkaStreamVehicleNoticeSender;

    /**
     * 闲置车辆判定, 达到闲置状态时长, 默认1天
     */
    private long idleTimeoutMillisecond = 86400000L;
    /**
     * 离线检查, 多长时间检查一下是否离线, 默认2分钟
     */
    private long offlineCheckSpanMillisecond = 120000L;
    /**
     * 离线判定, 多长时间算是离线, 默认10分钟
     */
    private long offlineTimeMillisecond = 600000L;
    /**
     * 最后进行离线检查的时间, 用于离线判断
     */
    private long lastOfflineCheckTimeMillisecond;

    /**
     * 车辆规则处理
     */
    private transient CarRuleHandler carRuleHandler;

    /**
     * 车辆上下线及相关处理
     */
    private transient CarOnOffHandler carOnOffhandler;

    /**
     * 故障码处理
     */
    private transient FaultCodeHandler faultCodeHandler;

    /**
     * 最后一次同步配置时间
     */
    private transient long lastUpdateConfigTime = 0;

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        this.collector = collector;

        carRuleHandler = new CarRuleHandler();
        carOnOffhandler = new CarOnOffHandler();
        faultCodeHandler = new FaultCodeHandler();

        prepareStreamSender(stormConf, collector);

        prepareStreamReceiver();

        prepareIdleVehicleThread(stormConf);
    }

    private void prepareStreamSender(
        @NotNull final Map stormConf,
        @NotNull final OutputCollector collector) {

        kafkaStreamSenderBuilder = KAFKA_STREAM.prepareSender(KAFKA_STREAM_ID, collector);

        // 输出到Kafka的主题
        final String noticeTopic = stormConf.get(SysDefine.KAFKA_TOPIC_NOTICE).toString();

        kafkaStreamVehicleNoticeSender = kafkaStreamSenderBuilder.build(noticeTopic);
    }

    private void prepareStreamReceiver() {

        dataStreamReceiver = FilterBolt.prepareDataStreamReceiver(this::executeFromDataStream);
    }

    @SuppressWarnings("AlibabaMethodTooLong")
    private void prepareIdleVehicleThread(
        @NotNull final Map stormConf) {

        final long currentTimeMillis = System.currentTimeMillis();
        lastOfflineCheckTimeMillisecond = currentTimeMillis;

        final ParamsRedisUtil paramsRedisUtil = ParamsRedisUtil.getInstance();
        paramsRedisUtil.rebulid();
        // 从Redis读取车辆闲置超时时间
        final Object idleTimeoutMillisecond = paramsRedisUtil.PARAMS.get(
            ParamsRedisUtil.VEHICLE_IDLE_TIMEOUT_MILLISECOND);
        if (null != idleTimeoutMillisecond) {
            this.idleTimeoutMillisecond = NumberUtils.toLong(
                idleTimeoutMillisecond.toString()
            );
        }

        // 多长时间算是离线
        final String offLineSecond = MapUtils.getString(stormConf, StormConfigKey.REDIS_OFFLINE_SECOND);
        if (!StringUtils.isEmpty(offLineSecond)) {
            offlineTimeMillisecond = Long.parseLong(org.apache.commons.lang.math.NumberUtils.isNumber(offLineSecond) ? offLineSecond : "0") * 1000;
        }

        // 多长时间检查一下是否离线
        final String offLineCheckSpanSecond = MapUtils.getString(stormConf, StormConfigKey.REDIS_OFFLINE_CHECK_SPAN_SECOND);
        if (!StringUtils.isEmpty(offLineCheckSpanSecond)) {
            offlineCheckSpanMillisecond = Long.parseLong(org.apache.commons.lang.math.NumberUtils.isNumber(offLineCheckSpanSecond) ? offLineCheckSpanSecond : "0") * 1000;
        }

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

        final long currentTimeMillis = System.currentTimeMillis();

        // 每分钟同步一次配置
        if(currentTimeMillis - lastUpdateConfigTime > TimeUnit.MINUTES.toMillis(1)) {

            lastUpdateConfigTime = currentTimeMillis;

            try {
                // 更新配置
                CarRuleHandler.rebulid();
                //从配置文件中读出超时时间
                Object idleTimeoutMillisecond = ParamsRedisUtil.getInstance().PARAMS.get(
                    ParamsRedisUtil.VEHICLE_IDLE_TIMEOUT_MILLISECOND);
                if (null != idleTimeoutMillisecond) {
                    this.idleTimeoutMillisecond = NumberUtils.toLong(
                        idleTimeoutMillisecond.toString()
                    );
                }
            } catch (Exception e) {
                LOG.error("同步配置异常", e);
            }
        }

    }

    @SuppressWarnings("AlibabaMethodTooLong")
    private void executeFromDataStream(
        @NotNull final Tuple input,
        @NotNull final String vid,
        @NotNull final ImmutableMap<String, String> data) {

        collector.ack(input);

        final long currentTimeMillis = System.currentTimeMillis();

        // region 离线判断: 如果时间差大于离线检查时间，则进行离线检查, 如果车辆离线，则发送此车辆的所有故障码结束通知
        if (currentTimeMillis - lastOfflineCheckTimeMillisecond >= offlineCheckSpanMillisecond) {

            lastOfflineCheckTimeMillisecond = currentTimeMillis;
            List<Map<String, Object>> msgs = faultCodeHandler.generateNotice(currentTimeMillis);

            if (null != msgs && msgs.size() > 0) {
                for (Map<String, Object> map : msgs) {
                    if (null != map && map.size() > 0) {
                        Object vid2 = map.get("vid");
                        String json = JSON_UTILS.toJson(map);
                        kafkaStreamVehicleNoticeSender.emit((String) vid2, json);
                    }
                }
            }

            //检查所有车辆是否离线，离线则发送离线通知。
            msgs = carRuleHandler.offlineMethod(currentTimeMillis);
            if (null != msgs && msgs.size() > 0) {
                for (Map<String, Object> map : msgs) {
                    if (null != map && map.size() > 0) {
                        Object vid2 = map.get("vid");
                        String json = JSON_UTILS.toJson(map);
                        kafkaStreamVehicleNoticeSender.emit((String) vid2, json);
                    }
                }
            }

            carOnOffhandler.onOffCheck("TIMEOUT", 1, currentTimeMillis, offlineTimeMillisecond);
        }
        // endregion

        if (MapUtils.isEmpty(data)) {
            return;
        }

        PARAMS_REDIS_UTIL.autoLog(vid, () -> LOG.warn("VID[{}]进入车辆通知处理", vid));


        // region 缓存有效状态

        if(DataUtils.isNotAutoWake(data)) {
            VEHICLE_CACHE.updateUsefulCache(data);
        }

        // endregion 缓存有效状态

        // region 更新实时缓存
        try {
            final String type = data.get(DataKey.MESSAGE_TYPE);
            if (!CommandType.SUBMIT_LINKSTATUS.equals(type)) {
                SysRealDataCache.updateCache(data, currentTimeMillis, idleTimeoutMillisecond);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // endregion

        //返回车辆通知,(重要)
        //先检查规则是否启用，启用了，则把dat放到相应的处理方法中。将返回结果放到list中，返回。
        List<Map<String, Object>> msgs = carRuleHandler.generateNotices(data);
        for (Map<String, Object> map : msgs) {
            if (null != map && map.size() > 0) {
                String json = JSON_UTILS.toJson(map);
                kafkaStreamVehicleNoticeSender.emit(vid, json);
            }
        }

        final List<Map<String, Object>> faultCodeMessages = faultCodeHandler.generateNotice(data);
        if (CollectionUtils.isNotEmpty(faultCodeMessages)) {
            for (Map<String, Object> map : faultCodeMessages) {
                if (null != map && map.size() > 0) {
                    String json = JSON_UTILS.toJson(map);
                    kafkaStreamVehicleNoticeSender.emit(vid, json);
                }
            }
        }
        //如果下线了，则发送上下线的里程值
        Map<String, Object> map = carOnOffhandler.generateNotices(data, currentTimeMillis, offlineTimeMillisecond);
        if (null != map && map.size() > 0) {
            String json = JSON_UTILS.toJson(map);
            kafkaStreamVehicleNoticeSender.emit(vid, json);
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