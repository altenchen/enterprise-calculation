package storm.bolt.deal.norm;

import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import storm.handler.cal.EsRealCalHandler;
import storm.kafka.spout.RegisterKafkaSpout;
import storm.stream.IStreamReceiver;
import storm.stream.KafkaStream;
import storm.stream.StreamReceiverFilter;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("all")
public class SynEsculBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1700001L;

    // region Component

    @NotNull
    private static final String COMPONENT_ID = SynEsculBolt.class.getSimpleName();

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

    private static final ConfigUtils CONFIG_UTILS = ConfigUtils.getInstance();
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private OutputCollector collector;

    private KafkaStream.SenderBuilder kafkaStreamSenderBuilder;

    private KafkaStream.Sender kafkaStreamElasticSearchStatusSender;

    private static String statusEsTopic;
    private long lastExeTime;
    private long offlinechecktime;
    private EsRealCalHandler handler;
    public static ScheduledExecutorService service;
    private static int ispreCp = 0;


    private StreamReceiverFilter registerStreamReceiver;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;

        statusEsTopic = stormConf.get(SysDefine.KAFKA_TOPIC_ES_STATUS).toString();

        prepareStreamSender(collector);

        registerStreamReceiver = RegisterKafkaSpout.prepareRegisterStreamReceiver(this::executeFromKafkaRegisterStream);

        long now = System.currentTimeMillis();
        lastExeTime = now;
        offlinechecktime = Long.parseLong(stormConf.get("offline.check.time").toString());
        try {
            if (stormConf.containsKey("redis.cluster.data.syn")) {
                Object precp = stormConf.get("redis.cluster.data.syn");
                if (null != precp && !"".equals(precp.toString().trim())) {
                    String str = precp.toString();
                    ispreCp = Integer.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(str) ? str : "0");
                }
            }
            handler = new EsRealCalHandler();
            if (5 == ispreCp || 2 == ispreCp) {
                // carinfo中车辆的注册信息 是否可以监控并推送 es, 此方法在系统启动的时候调用一次
                List<Map<String, Object>> monitormsgs = handler.redisCarInfoSendMsgs();
                if (null != monitormsgs && monitormsgs.size() > 0) {
                    System.out.println("---------------syn car is monitor or no--------total size:" + monitormsgs.size());
                    for (Map<String, Object> map : monitormsgs) {
                        if (null != map && map.size() > 0) {
                            Object vid = map.get(SysDefine.UUID);
                            String json = JSON_UTILS.toJson(map);

                            kafkaStreamElasticSearchStatusSender.emit(ObjectUtils.toString(vid), json);
                        }
                    }
                }
            }
            if (2 == ispreCp) {
                // 集群中的数据一次全量推送通知，此方法只会调用一次
                List<Map<String, Object>> msgs = handler.redisClusterSendMsgs();
                if (null != msgs && msgs.size() > 0) {
                    System.out.println("---------------syn redis cluster data--------");
                    for (Map<String, Object> map : msgs) {
                        if (null != map && map.size() > 0) {
                            Object vid = map.get(SysDefine.UUID);
                            String json = JSON_UTILS.toJson(map);

                            kafkaStreamElasticSearchStatusSender.emit(ObjectUtils.toString(vid), json);
                        }
                    }
                }

            }
            ispreCp = 1;

            class AliveCarOff implements Runnable {

                @Override
                public void run() {
                    try {
                        // 每调用一次此方法会批量的检查 所有在线的车辆 是否存在离线的情况
                        // 离线处理：发送es离线消息，将其在在线的车辆缓存中移除
                        List<Map<String, Object>> msgs = handler.checkAliveCarOffline();
                        if (null != msgs && msgs.size() > 0) {
                            for (Map<String, Object> map : msgs) {
                                if (null != map && map.size() > 0) {
                                    Object vid = map.get(SysDefine.UUID);
                                    String json = JSON_UTILS.toJson(map);

                                    kafkaStreamElasticSearchStatusSender.emit(ObjectUtils.toString(vid), json);
                                }
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new AliveCarOff(), 0, offlinechecktime, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void prepareStreamSender(
        @NotNull final OutputCollector collector) {

        kafkaStreamSenderBuilder = KAFKA_STREAM.prepareSender(KAFKA_STREAM_ID, collector);

        kafkaStreamElasticSearchStatusSender = kafkaStreamSenderBuilder.build(statusEsTopic);
    }

    @Override
    public void execute(@NotNull final Tuple input) {
        final long now = System.currentTimeMillis();

        if (SysDefine.SYNES_GROUP.equals(input.getSourceStreamId())) {
            // 来自FilterBolt
            final String vid = input.getString(0);
            final Map<String, String> data = Maps.newHashMap((Map < String, String >) input.getValue(1));
            if (null == data.get(DataKey.VEHICLE_ID)) {
                data.put(DataKey.VEHICLE_ID, vid);
            }

            final Map<String, Object> esMap = handler.getSendEsMsgAndSetAliveLast(data, now);
            if (MapUtils.isNotEmpty(esMap)) {

                String json = JSON_UTILS.toJson(esMap);

                kafkaStreamElasticSearchStatusSender.emit(vid, json);
            }
        } else {
            registerStreamReceiver.execute(input);
        }
    }

    private void executeFromKafkaRegisterStream(
        @NotNull final Tuple input,
        @NotNull final String vehicleId,
        @NotNull final String frame) {

        if (null != frame && frame.length() > 26 && frame.indexOf(SysDefine.COMMA) > 0 && frame.indexOf(SysDefine.COLON) > 0) {
            String[] params = frame.split(SysDefine.COMMA);
            Map<String, String> regMsgMap = new TreeMap<>();
            for (String param : params) {
                if (null != param) {
                    String[] items = param.split(SysDefine.COLON);
                    if (2 == items.length) {
                        regMsgMap.put(new String(items[0]), new String(items[1]));
                    } else {
                        regMsgMap.put(new String(items[0]), "");
                    }
                }
            }
            if (regMsgMap.size() > 2) {
                //
                Map<String, Object> esMap = handler.getRegCarMsg(regMsgMap);
                if (null != esMap && esMap.size() > 0) {
                    Object vid = esMap.get(SysDefine.UUID);

                    String json = JSON_UTILS.toJson(esMap);

                    kafkaStreamElasticSearchStatusSender.emit(ObjectUtils.toString(vid), json);
                }
            }
        }
    }

    @Override
    public void declareOutputFields(@NotNull final OutputFieldsDeclarer declarer) {

        KAFKA_STREAM.declareOutputFields(KAFKA_STREAM_ID, declarer);
    }

}
