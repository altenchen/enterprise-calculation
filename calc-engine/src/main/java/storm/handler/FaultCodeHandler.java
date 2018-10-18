package storm.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleModelCache;
import storm.constant.FormatConstant;
import storm.dto.*;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.DataUtils;
import storm.util.ParamsRedisUtil;
import storm.util.dbconn.Conn;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 故障处理
 * @author wza
 */
public class FaultCodeHandler implements Serializable {

    private static final long serialVersionUID = 1143313278543030344L;

    private static final Logger LOG = LoggerFactory.getLogger(FaultCodeHandler.class);

    private static final ConfigUtils CONFIG_UTILS = ConfigUtils.getInstance();

    private static final ParamsRedisUtil PARAMS_REDIS_UTIL = ParamsRedisUtil.getInstance();

    /**
     * 从数据库拉取规则的时间间隔, 默认360秒
     */
    private static long dbFlushTimeSpanMillisecond = 360 * 1000;

    /**
     * 多长时间算是离线, 默认600秒
     */
    private static long offlineTimeMillisecond = 600 * 1000;

    static {

        String dbFlushTimeSpanSecond = CONFIG_UTILS.sysDefine.getProperty(SysDefine.DB_CACHE_FLUSH_TIME_SECOND);
        if (StringUtils.isNotEmpty(dbFlushTimeSpanSecond) && StringUtils.isNumeric(dbFlushTimeSpanSecond)) {
            dbFlushTimeSpanMillisecond = Long.parseLong(dbFlushTimeSpanSecond)*1000;
        }

        String offlineSecond = CONFIG_UTILS.sysDefine.getProperty(StormConfigKey.REDIS_OFFLINE_SECOND);
        if (StringUtils.isNotEmpty(offlineSecond) && StringUtils.isNumeric(offlineSecond)) {
            offlineTimeMillisecond = Long.parseLong(offlineSecond)*1000;
        }
    }

    /**
     * 最近一次从数据库拉取规则的时间
     */
    private long lastPullRuleTime = 0;

    /**
     * 处理一些数据库查询的事情
     */
    private final Conn conn = new Conn();

    // region 按字节解析
    /**
     * 故障码规则, 按时间周期从数据库拉取下来.
     */
    @SuppressWarnings("unchecked")
    @NotNull
    private Collection<FaultCodeByteRule> rules = CollectionUtils.EMPTY_COLLECTION;

    /**
     * vidRuleMsgs是每辆车的故障码信息缓存, <vid, <faultId, <exceptionId,<k,v>>>>
     */
    private final Map<String, Map<String, Map<String, Map<String,Object>>>> vidByteRuleMsg = new ConcurrentHashMap<>();
    // endregion

    // region 按位解析
    /**
     * 按位解析故障码规则, 目前会覆盖按字节解析规则
     * <fault_type, <faultId, fault>>
     */
    private Map<String, Map<String, FaultTypeSingleBit>> bitRules = MapUtils.EMPTY_MAP;

    /**
     * vidRuleMsgs是每辆车的故障码信息缓存, <vid, <faultId, <exceptionId, <k,v>>>>
     */
    private final Map<String, Map<String, Map<String, Map<String,Object>>>> vidBitRuleMsg = new ConcurrentHashMap<>();

    // endregion 按位解析

    /**
     * 所有车辆的最后一帧报文的时间, <vid, lastFrameTimeMillisecond>
     */
    private Map<String, Long> lastTime = new ConcurrentHashMap<>();

    private final Object autoPullRulesLock = new Object();
    {
        try {
            autoPullRules();
        } catch (Exception e) {
            LOG.error("故障码处理初始化异常", e);
        }
    }

    /**
     * 初始化故障码报警规则
     */
    private void autoPullRules() {
        long requestTime = System.currentTimeMillis();
        if (requestTime - lastPullRuleTime > dbFlushTimeSpanMillisecond) {
            synchronized (autoPullRulesLock) {
                if(requestTime - lastPullRuleTime > dbFlushTimeSpanMillisecond) {
                    // 从数据库重新构建完整的规则
                    rules = conn.getFaultAlarmCodes();
                    bitRules = conn.getFaultSingleBitRules();
                    lastPullRuleTime = System.currentTimeMillis();
                }
            }
        }
    }

    private Collection<FaultCodeByteRule> getByteRules(){
        autoPullRules();
        return rules;
    }

    private Map<String, Map<String, FaultTypeSingleBit>> getBitRules(){
        autoPullRules();
        return bitRules;
    }
     
    public List<Map<String, Object>> generateNotice(long now){
        if (vidByteRuleMsg.size() == 0) {
            return null;
        }
        List<Map<String, Object>>notices = new LinkedList<>();
        String noticetime = DateFormatUtils.format(new Date(now), FormatConstant.DATE_FORMAT);
        //needRemoves缓存需要移除的故障码id（因为map不能在遍历的时候删除id或者放入id，否则会引发并发修改异常）
        List<String> needRemoves = new LinkedList<>();
        //lastTime为所有车辆的最后一帧报文的时间（vid，lastTime）
        for (final Map.Entry<String, Long> entry : lastTime.entrySet()) {
            final long last = entry.getValue();
            //如果这辆车已经离线，则把这辆车的故障码缓存移除，并且针对每个故障都发一个结束通知
            //offlinetime为车辆多长时间算是离线，
            if (now - last <= offlineTimeMillisecond) {
                continue;
            }

            final String vid = entry.getKey();
            lastTime.remove(vid);
            needRemoves.add(vid);

            // 每辆车的故障码信息缓存
            final Map<String, Map<String,Map<String,Object>>> vidByteNotices = vidByteRuleMsg.get(vid);
            if (MapUtils.isNotEmpty(vidByteNotices)) {

                for (final Map<String, Map<String, Object>> faultNotices : vidByteNotices.values()) {
                    if (!MapUtils.isNotEmpty(faultNotices)) {
                        continue;
                    }

                    for (Map<String, Object> exceptionNotice : faultNotices.values()) {
                        if (!MapUtils.isNotEmpty(exceptionNotice)) {
                            continue;
                        }

                        deleteNoticeMsg(
                            exceptionNotice,
                            noticetime,
                            "", noticetime);

                        notices.add(exceptionNotice);
                    }
                }
            }
            final Map<String, Map<String,Map<String,Object>>> vidBitNotices = vidBitRuleMsg.get(vid);
            if (MapUtils.isNotEmpty(vidBitNotices)) {

                for (final Map<String, Map<String, Object>> faultNotices : vidBitNotices.values()) {
                    if (!MapUtils.isNotEmpty(faultNotices)) {
                        continue;
                    }

                    for (Map<String, Object> exceptionNotice : faultNotices.values()) {
                        if (!MapUtils.isNotEmpty(exceptionNotice)) {
                            continue;
                        }

                        deleteNoticeMsg(
                            exceptionNotice,
                            noticetime,
                            "", noticetime);

                        notices.add(exceptionNotice);
                    }
                }
            }
        }

        for (final String vid : needRemoves) {
            vidByteRuleMsg.remove(vid);
            vidBitRuleMsg.remove(vid);
        }
        if (notices.size()>0) {
            return notices;
        }
        return null;
    }

    @NotNull
    public List<Map<String, Object>> generateNotice(@NotNull ImmutableMap<String, String> immutableMap) {
        final List<Map<String, Object>> notices = new LinkedList<>();

        if (MapUtils.isEmpty(immutableMap)) {
            return notices;
        }

        final Map<String, String> data = Maps.newHashMap(immutableMap);

        final String vid = data.get(DataKey.VEHICLE_ID);
        final String time = data.get(DataKey.TIME);

        if (StringUtils.isBlank(vid)
                || StringUtils.isBlank(time)) {
            return notices;
        }

        String latitude = data.get(DataKey._2503_LATITUDE);
        String longitude = data.get(DataKey._2502_LONGITUDE);
        String location = DataUtils.buildLocation(longitude, latitude);


        final long currentTimeMillis = System.currentTimeMillis();

        //获得最新的按单个位解析故障码告警规则
        final Map<String, Map<String, FaultTypeSingleBit>> bitRules = getBitRules();

        //获得最新的按字节解析故障码告警规则
        final Collection<FaultCodeByteRule> byteRules = getByteRules();

        // 目前只处理按1位解析规则, 否则走老规则

        //可充电储能故障码
        generateFaultMsg(notices, data, bitRules, byteRules, vid, time, location, currentTimeMillis, DataKey._2922);
        //驱动电机故障码
        generateFaultMsg(notices, data, bitRules, byteRules, vid, time, location, currentTimeMillis, DataKey._2805);
        //发动机故障码
        generateFaultMsg(notices, data, bitRules, byteRules, vid, time, location, currentTimeMillis, DataKey._2924);
        //其他故障(厂商扩展)
        generateFaultMsg(notices, data, bitRules, byteRules, vid, time, location, currentTimeMillis, DataKey._2809);
        //北汽故障码
        generateFaultMsg(notices, data, bitRules, byteRules, vid, time, location, currentTimeMillis, "4510003");

        return notices;
    }

    private void generateFaultMsg(
        @NotNull final List<Map<String, Object>> notices,
        @NotNull final Map<String, String> data,
        @NotNull final Map<String, Map<String, FaultTypeSingleBit>> bitRules,
        @NotNull final Collection<FaultCodeByteRule> byteRules,
        @NotNull final String vid,
        @NotNull final String time,
        @NotNull final String location,
        @NotNull final long currentTimeMillis,
        @NotNull final String faultType) {

        final String codeValues = data.get(faultType);
        if( StringUtils.isEmpty(codeValues) ){
            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                LOG.info("VID[{}]故障码为空, 忽略处理.", vid);
            });
            return;
        }
        final @NotNull long[] values = parseFaultCodes(codeValues);
        if(ArrayUtils.isEmpty(values)) {
            PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                LOG.info("VID[{}]故障码为空, 忽略处理.", vid);
            });
            return;
        }
        PARAMS_REDIS_UTIL.autoLog(vid, ()->{
            LOG.info("VID[{}]故障码解析[{}]:[{}]->[{}].", vid, faultType, ArrayUtils.toString(codeValues), ArrayUtils.toString(values));
        });

        // 车型, 空字符串代表没有配置, 只匹配默认规则
        final String vehModel = VehicleModelCache.getInstance().getVehicleModel(vid);

        PARAMS_REDIS_UTIL.autoLog(vid, ()->{
            LOG.info("VID[{}]解析车型为[{}], 故障类型[{}]", vid, vehModel, faultType);
        });
        String noticeTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

        boolean processByBit = false;
        if(bitRules.containsKey(faultType)) {
            final Map<String, FaultTypeSingleBit> faultTypeRules = bitRules.get(faultType);

            for (final Map.Entry<String, FaultTypeSingleBit> entry : faultTypeRules.entrySet()) {
                final String faultId = entry.getKey();
                final FaultTypeSingleBit faultTypeRule = entry.getValue();

                final Map<String, ExceptionSingleBit> exceptions = getVehicleExceptions(vehModel, faultTypeRule);

                if (MapUtils.isNotEmpty(exceptions)) {
                    processByBit = true;

                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        () -> LOG.info(
                            "VID[{}]故障类型[{}]按位解析, 一共[{}]条异常码.",
                            vid,
                            faultType,
                            exceptions.size()
                        )
                    );

                    // <faultId, <exceptionId, <k,v>>>
                    final Map<String, Map<String, Map<String, Object>>> vidNotice = vidBitRuleMsg.getOrDefault(
                        vid,
                        new ConcurrentHashMap<>());
                    vidBitRuleMsg.put(vid, vidNotice);

                    for (final ExceptionSingleBit bit : exceptions.values()) {
                        final String exceptionId = bit.exceptionId;
                        final long code = PartationBit.computeValue(values, bit.offset);

                        // <exceptionId, <k,v>>
                        final Map<String, Map<String, Object>> faultNotice = vidNotice.getOrDefault(
                            faultId,
                            new ConcurrentHashMap<>());
                        vidNotice.put(faultId, faultNotice);

                        lastTime.put(vid, currentTimeMillis);
                        if (code != 0) {
                            final Map<String, Object> exceptionNotice = updateNoticeMsg(
                                faultNotice.get(exceptionId),
                                vid,
                                time,
                                location,
                                exceptionId,
                                bit.level,
                                code,
                                noticeTime);
                            faultNotice.put(exceptionId, exceptionNotice);

                            final int status = (int) exceptionNotice.get(NOTICE_STATUS);
                            if (1 == status) {
                                notices.add(exceptionNotice);
                                PARAMS_REDIS_UTIL.autoLog(vid, () -> {
                                    LOG.info("VID[{}]按位解析EID[{}]触发", vid, exceptionId);
                                });
                            } else {
                                PARAMS_REDIS_UTIL.autoLog(vid, () -> {
                                    LOG.info("VID[{}]按位解析EID[{}]持续", vid, exceptionId);
                                });
                            }
                        } else {
                            final Map<String, Object> normalNotice = faultNotice.remove(exceptionId);
                            if (MapUtils.isNotEmpty(normalNotice)) {

                                deleteNoticeMsg(
                                    normalNotice,
                                    time,
                                    location,
                                    noticeTime);
                                notices.add(normalNotice);

                                PARAMS_REDIS_UTIL.autoLog(vid, () -> {
                                    LOG.info("VID[{}]按位解析EID[{}]解除", vid, exceptionId);
                                });
                            } else {
                                PARAMS_REDIS_UTIL.autoLog(vid, () -> {
                                    LOG.info("VID[{}]按位解析EID[{}]无效", vid, exceptionId);
                                });
                            }
                        }
                    }
                } else {
                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        () -> LOG.info(
                            "VID[{}]故障类型[{}]按位解析, 故障码[{}]没有异常码规则.",
                            vid,
                            faultId,
                            exceptions.size()
                        )
                    );
                }
            }
        } else {
            PARAMS_REDIS_UTIL.autoLog(
                vid,
                () -> LOG.info(
                    "VID[{}]故障类型[{}]没有按位解析规则",
                    vid,
                    faultType
                )
            );
        }

        // 没有匹配按位处理规则, 转为按字节处理
        if(!processByBit) {

            //添加车型判断
            final FaultCodeByteRule[] faultCodeByteRules = byteRules.stream()
                .filter(r -> StringUtils.equals(r.faultType, faultType) && r.effective(vehModel))
                .toArray(FaultCodeByteRule[]::new);

            PARAMS_REDIS_UTIL.autoLog(
                vid,
                () -> LOG.info(
                    "VID[{}]故障类型[{}]按值解析, 一共[{}]组故障规则.",
                    vid,
                    faultType,
                    faultCodeByteRules.length
                )
            );

            for (FaultCodeByteRule rule: faultCodeByteRules) {

                PARAMS_REDIS_UTIL.autoLog(
                    vid,
                    () -> LOG.info(
                        "VID[{}]故障类型[{}]按值解析, 故障码[{}]共[{}]个异常码.",
                        vid,
                        faultType,
                        rule.faultId,
                        rule.getFaultCodes().size()
                    )
                );

                List<Map<String, Object>> msgs = byteFaultMsg(data, values, rule);
                if (null != msgs) {
                    notices.addAll(msgs);
                }
            }
        }
    }

    private static Map<String, ExceptionSingleBit> getVehicleExceptions(
        @NotNull final String vehModel,
        final FaultTypeSingleBit faultTypeRule) {

        final Map<String, Map<String, ExceptionSingleBit>> vehExceptions = faultTypeRule.vehExceptions;
        final Map<String, ExceptionSingleBit> exceptions = vehExceptions.containsKey(vehModel)
            ? vehExceptions.get(vehModel)
            : vehExceptions.get("");
        return exceptions;
    }

    @NotNull
    public static long[] parseFaultCodes(@Nullable String faultCodes) {
        if(StringUtils.isBlank(faultCodes)) {
            return new long[0];
        }
        final String[] intStrings = StringUtils.split(faultCodes, '|');
        final long[] result = new long[intStrings.length];
        for (int i = 0; i < intStrings.length; i++) {
            result[i] = Long.decode(intStrings[i]);
        }
        return result;
    }

    /**
     * @param data 实时数据
     * @param msgFcodes 故障码
     * @param byteRules 故障码告警规则
     * @return 故障码告警
     */
    private List<Map<String, Object>> byteFaultMsg(
        @NotNull final Map<String, String> data,
        @NotNull final long[] msgFcodes,
        final FaultCodeByteRule byteRules) {

        final List<Map<String, Object>> notices = new LinkedList<>();

        final String vid = data.get(DataKey.VEHICLE_ID);
        final String time = data.get(DataKey.TIME);

        if (StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)) {
            return notices;
        }

        final String latitude = data.get(DataKey._2503_LATITUDE);
        final String longitude = data.get(DataKey._2502_LONGITUDE);
        final String location = DataUtils.buildLocation(longitude, latitude);

        final long currentTimeMillis = System.currentTimeMillis();
        final String noticetime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

        final Map<String, Map<String, Map<String,Object>>> vidNotices = vidByteRuleMsg.getOrDefault(vid, new ConcurrentHashMap<>());
        vidByteRuleMsg.put(vid, vidNotices);

        final String faultId = byteRules.faultId;

        final Map<String, Map<String,Object>> faultNotices = vidNotices.getOrDefault(faultId, new ConcurrentHashMap<>());
        vidNotices.put(faultId, faultNotices);

        //codes为若干个数字, 包含正常码和异常码集合
        final Iterable<FaultCodeByte> rules = byteRules.getFaultCodes();
        boolean hasExceptionCode = false;
        // region 先处理异常码
        for (final FaultCodeByte exceptionRule : rules) {
            if(0 == exceptionRule.type) {
                //跳过正常码
                continue;
            }
            final long exceptionCode = Long.decode(exceptionRule.equalCode);
            if (!ArrayUtils.contains(msgFcodes, exceptionCode)) {
                continue;
            }
            lastTime.put(vid, currentTimeMillis);
            hasExceptionCode = true;

            final String exceptionId = exceptionRule.codeId;
            // 一个故障信息, 表示这个故障码是否触发
            final Map<String, Object> notice = updateNoticeMsg(
                faultNotices.get(exceptionId),
                vid,
                time,
                location,
                exceptionId,
                exceptionRule.alarmLevel,
                exceptionCode,
                noticetime);

            //添加通知消息
            if(1 == (int)notice.get(NOTICE_STATUS)) {
                notices.add(notice);
                PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                    LOG.info("VID[{}]按值解析EID[{}]触发", vid, exceptionId);
                });
            }
            //添加缓存
            faultNotices.put(exceptionId, notice);
        }
        // endregion
        // region 当没有异常码时, 才处理正常码.
        if (hasExceptionCode) {
            LOG.info("VID[{}]按值解析FID[{}], 异常码和正常码同时出现, 忽略正常码.", vid, faultId);
            return notices;
        }
        for (final FaultCodeByte normalRule : rules) {
            if(1 == normalRule.type) {
                //跳过异常码
                continue;
            }
            final long normalCode = Long.decode(normalRule.equalCode);
            if (!ArrayUtils.contains(msgFcodes, normalCode)) {
                continue;
            }
            lastTime.put(vid, currentTimeMillis);

            if (MapUtils.isEmpty(faultNotices)) {
                continue;
            }

            for (final String exceptionId : faultNotices.keySet()) {

                final Map<String, Object> notice = faultNotices.get(exceptionId);

                if (MapUtils.isNotEmpty(notice)) {

                    deleteNoticeMsg(
                        notice,
                        time,
                        location,
                        noticetime);
                    notices.add(notice);
                    PARAMS_REDIS_UTIL.autoLog(vid, ()->{
                        LOG.info("VID[{}]按值解析EID[{}]解除", vid, exceptionId);
                    });
                }

                faultNotices.remove(exceptionId);

            }
        }
        // endregion

        return notices;
    }

    private static final String NOTICE_STATUS = "status";
    private static final String NOTICE_LEVEL = "level";

    @NotNull
    private Map<String,Object> updateNoticeMsg(
        @Nullable Map<String, Object> notice,
        @NotNull final String vid,
        final String time,
        final String location,
        final String exceptionId,
        final int alarmLevel,
        final long faultCode,
        final String noticeTime) {

        if(MapUtils.isEmpty(notice)) {
            notice = new HashMap<>();
            notice.put("msgType", "FAULT_CODE_ALARM");
            notice.put("msgId", UUID.randomUUID().toString());
            notice.put("vid", vid);
        }

        if (!notice.containsKey(NOTICE_LEVEL) || (int) notice.get(NOTICE_LEVEL) != alarmLevel) {
            notice.put(NOTICE_STATUS, 1);
            notice.put("stime", time);
            notice.put("slocation", location);
            notice.put(NOTICE_LEVEL, alarmLevel);
        } else {
            notice.put(NOTICE_STATUS, 2);
        }

        notice.put("ruleId", exceptionId);
        notice.put("faultCode", faultCode);
        notice.put("noticetime", noticeTime);

        return notice;
    }

    private void deleteNoticeMsg(
        @NotNull final Map<String, Object> notice,
        final String time,
        final String location,
        final String noticeTime) {

        notice.put(NOTICE_STATUS, 3);
        notice.put("etime", time);
        notice.put("elocation", location);
        notice.put("noticetime", noticeTime);
    }
}
