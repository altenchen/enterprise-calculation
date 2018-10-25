package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;
import storm.constant.FormatConstant;
import storm.constant.RedisConstant;
import storm.dao.DataToRedis;
import storm.handler.ctx.Recorder;
import storm.handler.ctx.RedisRecorder;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.DataUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author: xzp
 * @date: 2018-06-14
 * @description: 无CAN车辆审计者
 */
public final class CarNoCanJudge {

    private static final Logger logger = LoggerFactory.getLogger(CarNoCanJudge.class);
    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();
    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final JsonUtils GSON_UTILS = JsonUtils.getInstance();

    private static final int REDIS_DB_INDEX = 6;

    /**
     * 出于兼容性考虑暂留, 已存储到车辆缓存<code>VehicleCache</code>中
     */
    private static final String REDIS_TABLE_NAME = "vehCache.qy.notice.can";

    private static final String STATUS_KEY = "status";

    /**
     * CAN故障首帧计数值
     */
    private static final byte FIRST_FAULT_FRAME = -1;

    /**
     * CAN正常首帧计数值
     */
    private static final byte FIRST_NORMAL_FRAME = 1;

    /**
     * 触发CAN故障需要的连续帧数
     */
    private static int faultTriggerContinueCount = 3;

    /**
     * 触发CAN故障需要的持续时长
     */
    private static long faultTriggerTimeoutMillisecond = 0;

    /**
     * 触发CAN正常需要的连续帧数
     */
    private static int normalTriggerContinueCount = 3;

    /**
     * 触发CAN正常需要的持续时长
     */
    private static long normalTriggerTimeoutMillisecond = 0;

    /**
     * 判断车辆无CAN的方案
     */
    private static ICarNoCanDecide carNoCanDecide = new CarNoCanDecideDefault();

    private final DataToRedis redis = new DataToRedis();
    private final Recorder recorder = new RedisRecorder(redis);

    /**
     * 无CAN车辆字典
     */
    private final Map<String, CarNoCanItem> carNoCanMap = new TreeMap<>();

    // 实例初始化代码块
    {

        JEDIS_POOL_UTILS.useResource(jedis -> {

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.equals(select)) {
                return;
            }

            final Map<String, String> notices = jedis.hgetAll(REDIS_TABLE_NAME);
            for (String vid : notices.keySet()) {

                final String json = notices.get(vid);
                final Map<String, String> notice = GSON_UTILS.fromJson(
                    json,
                    new TypeToken<TreeMap<String, String>>() {
                    }.getType());

                try {
                    final String statusString = notice.get(STATUS_KEY);
                    final int statusValue = NumberUtils.toInt(statusString);
                    final AlarmStatus status = AlarmStatus.parseOf(statusValue);

                    if (AlarmStatus.Start == status || AlarmStatus.Continue == status) {
                        final CarNoCanItem carNoCanItem = new CarNoCanItem(vid, notice, status);
                        carNoCanMap.put(carNoCanItem.vid, carNoCanItem);
                    }
                } catch (Exception ignore) {
                    logger.warn("初始化告警异常", ignore);
                }
            }
        });
    }

    // region 全局配置

    /**
     * 获取触发CAN故障需要的连续帧数
     */
    @Contract(pure = true)
    public static int getFaultTriggerContinueCount() {
        return faultTriggerContinueCount;
    }

    /**
     * 设置触发CAN故障需要的连续帧数
     */
    public static void setFaultTriggerContinueCount(int faultTriggerContinueCount) {
        CarNoCanJudge.faultTriggerContinueCount = faultTriggerContinueCount;
        logger.info("触发CAN故障需要的连续帧数被设置为:" + faultTriggerContinueCount);
    }

    /**
     * 获取触发CAN故障需要的持续时长
     */
    @Contract(pure = true)
    public static long getFaultTriggerTimeoutMillisecond() {
        return faultTriggerTimeoutMillisecond;
    }

    /**
     * 设置触发CAN故障需要的持续时长
     */
    public static void setFaultTriggerTimeoutMillisecond(long faultTriggerTimeoutMillisecond) {
        CarNoCanJudge.faultTriggerTimeoutMillisecond = faultTriggerTimeoutMillisecond;
        logger.info("触发CAN故障需要的持续时长被设置为:" + faultTriggerTimeoutMillisecond);
    }

    /**
     * 获取触发CAN正常需要的连续帧数
     */
    @Contract(pure = true)
    public static int getNormalTriggerContinueCount() {
        return normalTriggerContinueCount;
    }

    /**
     * 设置触发CAN正常需要的连续帧数
     */
    public static void setNormalTriggerContinueCount(int normalTriggerContinueCount) {
        CarNoCanJudge.normalTriggerContinueCount = normalTriggerContinueCount;
        logger.info("触发CAN正常需要的连续帧数被设置为:" + normalTriggerContinueCount);
    }

    /**
     * 获取触发CAN正常需要的持续时长
     */
    @Contract(pure = true)
    public static long getNormalTriggerTimeoutMillisecond() {
        return normalTriggerTimeoutMillisecond;
    }

    /**
     * 设置触发CAN正常需要的持续时长
     */
    public static void setNormalTriggerTimeoutMillisecond(long normalTriggerTimeoutMillisecond) {
        CarNoCanJudge.normalTriggerTimeoutMillisecond = normalTriggerTimeoutMillisecond;
        logger.info("触发CAN正常需要的持续时长被设置为:" + normalTriggerTimeoutMillisecond);
    }

    // endregion

    /**
     * @param data 车辆数据
     * @return 如果产生无CAN通知, 则填充通知, 否则为空集合.
     */
    @NotNull
    public Map<String, Object> processFrame(@NotNull Map<String, String> data) {
        // vid, notice
        final TreeMap<String, Object> result = new TreeMap<>();

        final String vid = data.get(DataKey.VEHICLE_ID);
        final String timeString = data.get(DataKey.TIME);

        if (StringUtils.isBlank(vid)
            || StringUtils.isBlank(timeString)) {
            logger.info("vid:{} 无CAN放弃判定, 时间空白.", vid);
            return result;
        }
        final long time;
        try {
            time = DateUtils.parseDate(timeString, new String[]{FormatConstant.DATE_FORMAT}).getTime();
        } catch (ParseException e) {
            logger.warn("无CAN放弃判定, 时间格式错误.", e);
            return result;
        }

        // 总里程
        final String totalMileage = data.get(DataKey._2202_TOTAL_MILEAGE);
        // 经度
        final String longitude = data.get(DataKey._2502_LONGITUDE);
        // 纬度
        final String latitude = data.get(DataKey._2503_LATITUDE);
        // 经纬度坐标
        final String location = DataUtils.buildLocation(longitude, latitude);


        // 判断是否有CAN
        final ICarNoCanDecide carNoCanDecide = getCarNoCanDecide();
        boolean hasCan = carNoCanDecide.hasCan(data);

        if (!hasCan) {
            final CarNoCanItem carNoCanItem;
            if (carNoCanMap.containsKey(vid)) {
                carNoCanItem = carNoCanMap.get(vid);
            } else {
                carNoCanItem = new CarNoCanItem(vid);
                carNoCanMap.put(vid, carNoCanItem);
            }
            continueFaultIncrement(result, carNoCanItem, time, totalMileage, location);
        } else if (carNoCanMap.containsKey(vid)) {
            final CarNoCanItem carNoCanItem = carNoCanMap.get(vid);
            continueNormalIncrement(result, carNoCanItem, time, totalMileage, location);
        }

        if (result.containsKey(STATUS_KEY)) {
            logger.info("VID[{}]收到无CAN告警通知, status[{}]", vid, result.get(STATUS_KEY));
        }

        return result;
    }

    /**
     * 连续CAN故障自增
     */
    private void continueFaultIncrement(
        @NotNull final Map<String, Object> noticeResult,
        @NotNull final CarNoCanItem item,
        @NotNull final long time,
        @Nullable final String totalMileage,
        @NotNull final String location
    ) {

        if (item.continueCount > FIRST_FAULT_FRAME) {
            item.continueCount = FIRST_FAULT_FRAME;
        }

        if (item.continueCount == FIRST_FAULT_FRAME) {

            final String totalMileageString;
            try {

                totalMileageString = getTotalMileageString(item.vid, totalMileage);
            } catch (ExecutionException e) {

                logger.warn("从缓存获取有效累计里程值异常", e);
                return;
            }

            item.firstFrameEnterFault(time, totalMileageString, location);
        }

        logger.info("VID[" + item.vid + "]判定为无CAN[" + item.continueCount + "]");

        if (item.continueCount > -getFaultTriggerContinueCount()) {
            --item.continueCount;
            return;
        }

        final long delayMillisecond = getFaultTriggerTimeoutMillisecond();
        if (time - item.getFirstFrameTime() > delayMillisecond) {

            item.lastFrameEnterFault(delayMillisecond);

            if (item.getAlarmStatus() == AlarmStatus.End
                || item.getAlarmStatus() == AlarmStatus.Init) {

                try {

                    final ImmutableMap<String, String> oldNotice = VEHICLE_CACHE.getField(
                        item.vid,
                        NoticeType.NO_CAN_VEH
                    );
                    if (MapUtils.isNotEmpty(oldNotice)) {

                        final String vid = oldNotice.get("vid");
                        final int status = NumberUtils.toInt(oldNotice.get(STATUS_KEY));
                        if (item.vid.equals(vid)
                            && (status == AlarmStatus.Start.value || status == AlarmStatus.Continue.value)) {

                            final CarNoCanItem newItem = new CarNoCanItem(
                                item.vid,
                                oldNotice,
                                AlarmStatus.parseOf(status));
                            carNoCanMap.put(newItem.vid, newItem);
                            logger.info("从缓存取回CAN故障告警, 不再发送通知.");
                            return;
                        }
                    }
                } catch (ExecutionException e) {

                    logger.warn("获取CAN故障通知缓存异常");
                }

                item.setAlarmStatus(AlarmStatus.Start);

                logger.info("VID[" + item.vid + "]触发CAN故障[" + item.getAlarmStatus() + "]");

                final ImmutableMap<String, String> notice = item.generateNotice();

                noticeResult.putAll(notice);

                try {
                    VEHICLE_CACHE.putField(
                        item.vid,
                        NoticeType.NO_CAN_VEH,
                        notice
                    );
                    logger.info("VID[{}]CAN故障, 更新缓存.", item.vid);
                } catch (ExecutionException e) {
                    logger.error("存储CAN故障通知缓存异常");
                }
            }
        }
    }

    /**
     * 连续CAN正常自增
     */
    private void continueNormalIncrement(
        @NotNull final Map<String, Object> noticeResult,
        @NotNull final CarNoCanItem item,
        @NotNull final long time,
        @Nullable final String totalMileage,
        @NotNull final String location
    ) {
        if (item.continueCount < FIRST_NORMAL_FRAME) {
            item.continueCount = FIRST_NORMAL_FRAME;
        }

        if (item.continueCount == FIRST_NORMAL_FRAME) {

            final String totalMileageString;
            try {

                totalMileageString = getTotalMileageString(item.vid, totalMileage);
            } catch (ExecutionException e) {

                logger.warn("从缓存获取有效累计里程值异常", e);
                return;
            }

            item.firstFrameLeaveFault(time, totalMileageString, location);
        }

        logger.info("VID[" + item.vid + "]判定为有CAN[" + item.continueCount + "]");

        if (item.continueCount < getNormalTriggerContinueCount()) {
            ++item.continueCount;
            return;
        }

        final long delayMillisecond = getNormalTriggerTimeoutMillisecond();
        if (time - item.getFirstFrameTime() > delayMillisecond) {
            item.lastFrameLeaveFault(delayMillisecond);

            if (item.getAlarmStatus() == AlarmStatus.Start
                || item.getAlarmStatus() == AlarmStatus.Continue) {

                try {

                    final ImmutableMap<String, String> oldNotice = VEHICLE_CACHE.getField(
                        item.vid,
                        NoticeType.NO_CAN_VEH
                    );
                    if (MapUtils.isNotEmpty(oldNotice)) {

                        final String vid = oldNotice.get("vid");
                        final int status = NumberUtils.toInt(oldNotice.get(STATUS_KEY));
                        if (item.vid.equals(vid)
                            && (status == AlarmStatus.End.value || status == AlarmStatus.Init.value)) {

                            final CarNoCanItem newItem = new CarNoCanItem(
                                item.vid,
                                oldNotice,
                                AlarmStatus.parseOf(status));
                            carNoCanMap.put(newItem.vid, newItem);
                            logger.info("从缓存取回CAN正常告警, 不再发送通知.");
                            return;
                        }
                    }
                } catch (ExecutionException e) {

                    logger.warn("获取CAN正常通知缓存异常");
                }

                item.setAlarmStatus(AlarmStatus.End);

                logger.info("VID[" + item.vid + "]触发CAN正常[" + item.getAlarmStatus() + "]");

                final ImmutableMap<String, String> notice = item.generateNotice();

                noticeResult.putAll(notice);

                try {
                    VEHICLE_CACHE.delField(
                        item.vid,
                        NoticeType.NO_CAN_VEH
                    );
                    logger.info("VID[{}]CAN正常, 删除缓存.", item.vid);
                } catch (ExecutionException e) {
                    logger.error("删除CAN正常通知缓存异常");
                }

                // 出于兼容性考虑, 先暂留.
                recorder.del(REDIS_DB_INDEX, REDIS_TABLE_NAME, item.vid);
            }

            carNoCanMap.remove(item.vid);
        }
    }

    @Contract(pure = true)
    public static ICarNoCanDecide getCarNoCanDecide() {
        return CarNoCanJudge.carNoCanDecide;
    }

    public static boolean setCarNoCanDecide(@NotNull ICarNoCanDecide carNoCanDecide) {
        assert carNoCanDecide != null : "车辆无CAN判定实现不能设置为空";
        final String beforeDecide = getCarNoCanDecide().getClass().getCanonicalName();
        final String afterDecide = carNoCanDecide.getClass().getCanonicalName();
        final String log = "车辆无CAN判定实现从[" + beforeDecide + "]切换到[" + afterDecide + "].";
        System.out.println("[console]" + log);
        logger.info("[logger]" + log);
        CarNoCanJudge.carNoCanDecide = carNoCanDecide;
        return true;
    }

    @NotNull
    private String getTotalMileageString(
        @NotNull String vid,
        @Nullable String totalMileage)
        throws ExecutionException {

        if (NumberUtils.isDigits(totalMileage)) {
            return totalMileage;
        } else {
            return VEHICLE_CACHE.getTotalMileageString(
                vid,
                "0");
        }
    }

    /**
     * 无CAN车辆被审计项
     */
    private static final class CarNoCanItem {

        private static final Logger logger = LoggerFactory.getLogger(CarNoCanItem.class);

        /**
         * 连续CAN计数, 0无效, 正数为正常计数, 负数为故障计数
         */
        public int continueCount;

        /**
         * 连续CAN首帧时间
         */
        private long firstFrameTime;

        /**
         * 告警状态
         */
        private AlarmStatus alarmStatus;

        /**
         * 车辆ID
         */
        public final String vid;

        /**
         * 属性集合
         */
        private final Map<String, String> properties;

        /**
         * 消息唯一ID
         */
        public final String msgId;

        /**
         * 正常运行时创建的项
         *
         * @param vid 车辆ID
         */
        public CarNoCanItem(String vid) {
            this.vid = vid;
            this.properties = new TreeMap<>();
            msgId = UUID.randomUUID().toString();

            // 车辆Id
            this.properties.put("vid", vid);
            // 消息类型
            this.properties.put("msgType", NoticeType.NO_CAN_VEH);
            // 消息唯一ID
            this.properties.put("msgId", msgId);
            // 消息状态
            this.properties.put(STATUS_KEY, String.valueOf(AlarmStatus.Init.value));

            this.setAlarmStatus(AlarmStatus.Init);
        }

        /**
         * Storm重启后从Redis还原项
         *
         * @param vid         车辆ID
         * @param properties  属性集合
         * @param alarmStatus 通知状态
         */
        public CarNoCanItem(String vid, ImmutableMap<String, String> properties, AlarmStatus alarmStatus) {
            this(vid, new TreeMap<>(properties), alarmStatus);
        }

        /**
         * Storm重启后从Redis还原项
         *
         * @param vid         车辆ID
         * @param properties  属性集合
         * @param alarmStatus 通知状态
         */
        public CarNoCanItem(String vid, Map<String, String> properties, AlarmStatus alarmStatus) {
            this.vid = vid;
            this.properties = properties;
            msgId = properties.get("msgId");

            // 车辆Id
            this.properties.put("vid", vid);
            // 消息状态
            this.properties.put(STATUS_KEY, String.valueOf(alarmStatus.value));
            this.setAlarmStatus(alarmStatus);
        }

        /**
         * 进入告警的首帧填充状态
         *
         * @param time
         * @param totalMileage
         * @param location
         */
        public final void firstFrameEnterFault(
            final long time,
            final String totalMileage,
            final String location
        ) {
            firstFrameTime = time;
            final String stime = DateFormatUtils.format(new Date(time), FormatConstant.DATE_FORMAT);
            properties.put("stime", stime);
            properties.put("smileage", totalMileage);
            properties.put("slocation", location);
        }

        /**
         * 进入告警的末帧填充状态
         *
         * @param delayMillisecond
         */
        public final void lastFrameEnterFault(final long delayMillisecond) {
            properties.put("sdelay", String.valueOf(delayMillisecond / 1000));
        }

        /**
         * 退出告警的首帧填充状态
         *
         * @param time
         * @param totalMileage
         * @param location
         */
        public final void firstFrameLeaveFault(
            final long time,
            final String totalMileage,
            final String location
        ) {
            firstFrameTime = time;
            final String etime = DateFormatUtils.format(new Date(time), FormatConstant.DATE_FORMAT);
            properties.put("etime", etime);
            properties.put("emileage", totalMileage);
            properties.put("elocation", location);
        }

        /**
         * 退出告警的末帧填充状态
         *
         * @param delayMillisecond
         */
        public final void lastFrameLeaveFault(final long delayMillisecond) {
            properties.put("edelay", String.valueOf(delayMillisecond / 1000));
        }

        /**
         * 根据当前属性生成通知消息
         *
         * @return 通知消息
         */
        @NotNull
        public final ImmutableMap<String, String> generateNotice() {
            final Date now = new Date();
            final String noticeTime = DateFormatUtils.format(now, FormatConstant.DATE_FORMAT);

            this.properties.put("noticetime", noticeTime);
            final ImmutableMap<String, String> result = new ImmutableMap.Builder<String, String>()
                .putAll(this.properties)
                .build();
            return result;
        }

        /**
         * @return 连续CAN首帧时间
         */
        public long getFirstFrameTime() {
            return firstFrameTime;
        }

        /**
         * 获取告警状态
         */
        public AlarmStatus getAlarmStatus() {
            return alarmStatus;
        }

        /**
         * 设置告警状态
         */
        public void setAlarmStatus(AlarmStatus alarmStatus) {
            if (this.alarmStatus == alarmStatus) {
                return;
            }

            logger.info("VID[{}]无CAN状态从[{}]切换到[{}]", vid, this.alarmStatus, alarmStatus);

            this.alarmStatus = alarmStatus;
            properties.put(STATUS_KEY, String.valueOf(alarmStatus.value));
        }
    }

    /**
     * 告警状态
     */
    private enum AlarmStatus {

        /**
         * 初始
         */
        Init(0),

        /**
         * 开始
         */
        Start(1),

        /**
         * 持续
         */
        Continue(2),

        /**
         * 结束
         */
        End(3),;

        public final int value;

        AlarmStatus(int value) {
            this.value = value;
        }

        @Contract(pure = true)
        public static AlarmStatus parseOf(int value) {
            if (Init.value == value) {
                return Init;
            }
            if (Start.value == value) {
                return Start;
            }
            if (Continue.value == value) {
                return Continue;
            }
            if (End.value == value) {
                return End;
            }

            throw new UnknownFormatConversionException("无法识别的选项");
        }

        @Contract(pure = true)
        public boolean equals(int value) {
            return this.value == value;
        }

        @Contract("null -> false")
        public boolean equals(String name) {
            return this.name().equals(name);
        }
    }
}
