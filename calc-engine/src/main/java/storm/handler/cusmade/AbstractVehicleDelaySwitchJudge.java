package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.ObjectExtension;
import storm.system.DataKey;
import storm.tool.MultiDelaySwitch;
import storm.util.DataUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;

/**
 * 实现了车辆基本的连续帧数统计处理
 * 持久化状态存储 内存 <==> redis
 *
 * @author 智杰
 */
public abstract class AbstractVehicleDelaySwitchJudge {

    //region 其他属性

    private static final Logger LOG = LoggerFactory.getLogger(AbstractVehicleDelaySwitchJudge.class);

    //endregion

    //region redis操作相关属性

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();

    /**
     * 默认存储在6库
     */
    private final int redisDb;

    protected static final String NOTICE_STATUS_KEY = "status";

    protected static final String NOTICE_START_STATUS = "1";

    protected static final String NOTICE_END_STATUS = "3";

    //endregion

    //region 延迟开关指标

    /**
     * 开始阈值连接帧数
     */
    private int beginTriggerContinueCount;

    /**
     * 开始阈值连续时长
     */
    private long beginTriggerTimeoutMillisecond;

    /**
     * 结束阈值连接帧数
     */
    private int endTriggerContinueCount;

    /**
     * 结束阈值连续时长
     */
    private long endTriggerTimeoutMillisecond;

    //endregion

    //region 内存缓存

    /**
     * 车辆状态缓存表 <vehicleId, DelaySwitch>
     */
    private final Map<String, MultiDelaySwitch<State>> vehicleStatus = Maps.newHashMap();

    /**
     * 车辆通知缓存表 <vehicleId, partNotice>
     */
    private final Map<String, ImmutableMap<String, String>> vehicleNoticeCache = Maps.newHashMap();

    //endregion

    public AbstractVehicleDelaySwitchJudge(
        int redisDb,
        int beginTriggerContinueCount,
        long beginTriggerTimeoutMillisecond,
        int endTriggerContinueCount,
        long endTriggerTimeoutMillisecond) {

        this.redisDb = redisDb;
        init(beginTriggerContinueCount, beginTriggerTimeoutMillisecond, endTriggerContinueCount, endTriggerTimeoutMillisecond);
    }

    public AbstractVehicleDelaySwitchJudge(
        int beginTriggerContinueCount,
        long beginTriggerTimeoutMillisecond,
        int endTriggerContinueCount,
        long endTriggerTimeoutMillisecond) {
        this(6, beginTriggerContinueCount, beginTriggerTimeoutMillisecond, endTriggerContinueCount, endTriggerTimeoutMillisecond);
    }

    //region 对外接口

    @NotNull
    public State getState(@NotNull final String vehicleId) {
        return ObjectExtension.defaultIfNull(
            ensureVehicleStatus(vehicleId).getSwitchStatus(),
            State.UNKNOWN
        );
    }

    /**
     * 处理实时数据
     *
     * @param data 报文
     * @return
     */
    @Nullable
    public String processFrame(@NotNull final String vehicleId, @NotNull final ImmutableMap<String, String> data) {
        beforeProcess(data);
        String result = privateProcessFrame(vehicleId, data);
        afterProcess(data);
        return result;
    }

    @Nullable
    private String privateProcessFrame(@NotNull final String vehicleId, @NotNull final ImmutableMap<String, String> data) {
        try {
            final String platformReceiverTimeString = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
            final long platformReceiverTime;
            try {
                platformReceiverTime = DataUtils.parseFormatTime(platformReceiverTimeString);
            } catch (@NotNull final ParseException e) {
                LOG.warn("解析服务器时间异常", e);
                return null;
            }

            //检查数据有效性
            if (ignore(data)) {
                return null;
            }

            switch (parseState(data)) {
                case BEGIN: {
                    //超过开始阈值
                    return processBeginFrame(
                        vehicleId,
                        platformReceiverTime,
                        data,
                        platformReceiverTimeString);
                }
                case END: {
                    //小于结束阈值
                    return processEndFrame(
                        vehicleId,
                        data,
                        platformReceiverTime,
                        platformReceiverTimeString);
                }
                default:
                    break;
            }
        } catch (Exception e) {
            LOG.error("VID:" + vehicleId + " + 处理数据异常", e);
        }
        return null;
    }

    @Nullable
    private String processEndFrame(
        final String vehicleId,
        final @NotNull ImmutableMap<String, String> data,
        final long platformReceiverTime,
        final String platformReceiverTimeString) {

        return ensureVehicleStatus(vehicleId).increase(
            State.END,
            platformReceiverTime,
            state -> {
                //初始化结束通知
                ImmutableMap<String, String> notice = initEndNotice(data, vehicleId, platformReceiverTimeString);
                vehicleNoticeCache.put(vehicleId, notice);
            },
            (state, count, timeout) -> {
                //发送结束通知
                Map<String, String> endNoticeMap = buildEndNotice(data, count, timeout, vehicleId);
                if (MapUtils.isNotEmpty(endNoticeMap)) {
                    final String json = JSON_UTILS.toJson(endNoticeMap);
                    //清除redis中的车辆通知
                    removeNoticeToRedis(vehicleId);
                    //清除内存中的车辆通知
                    vehicleNoticeCache.remove(vehicleId);
                    return json;
                }
                return null;
            }
        );
    }

    @Nullable
    private String processBeginFrame(
        final String vehicleId,
        final long platformReceiverTime,
        final @NotNull ImmutableMap<String, String> data,
        final String platformReceiverTimeString) {

        return ensureVehicleStatus(vehicleId).increase(
            State.BEGIN,
            platformReceiverTime,
            state -> {
                //初始化开始通知
                ImmutableMap<String, String> notice = initBeginNotice(data, vehicleId, platformReceiverTimeString);
                vehicleNoticeCache.put(vehicleId, notice);
            },
            (state, count, timeout) -> {
                //发送开始通知
                Map<String, String> startNotice = buildBeginNotice(data, count, timeout, vehicleId);
                if (MapUtils.isNotEmpty(startNotice)) {
                    final String json = JSON_UTILS.toJson(startNotice);
                    //写入redis
                    insertNoticeToRedis(vehicleId, json);
                    return json;
                }
                return null;
            }
        );
    }

    /**
     * 同步延迟开关指标
     *
     * @param beginTriggerContinueCount      开始阈值
     * @param beginTriggerTimeoutMillisecond 开始阈值连续时长
     * @param endTriggerContinueCount        结束阈值
     * @param endTriggerTimeoutMillisecond   结束阈值连续时长
     */
    @SuppressWarnings({"unused", "WeakerAccess"})
    public void syncDelaySwitchConfig(int beginTriggerContinueCount, int beginTriggerTimeoutMillisecond, int endTriggerContinueCount, int endTriggerTimeoutMillisecond) {
        init(beginTriggerContinueCount, beginTriggerTimeoutMillisecond, endTriggerContinueCount, endTriggerTimeoutMillisecond);
        vehicleStatus.forEach((vehicleId, delaySwitch) -> {
            delaySwitch
                .setThresholdTimes(State.BEGIN, this.beginTriggerContinueCount)
                .setTimeoutMillisecond(State.BEGIN, this.beginTriggerTimeoutMillisecond)
                .setThresholdTimes(State.END, this.endTriggerContinueCount)
                .setTimeoutMillisecond(State.END, this.endTriggerTimeoutMillisecond);
        });
    }
    //endregion

    //region 供子类调用

    /**
     * 读取redis中的通知
     *
     * @param vehicleId 车辆ID
     * @return
     */
    @NotNull
    protected ImmutableMap<String, String> readRedisVehicleNotice(@NotNull final String vehicleId) {
        ImmutableMap<String, String> result = JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(redisDb);
            String noticeKey = buildRedisKey();
            final String json = jedis.hget(noticeKey, vehicleId);
            if (StringUtils.isNotBlank(json)) {
                return ImmutableMap.copyOf(
                    ObjectExtension.defaultIfNull(
                        JSON_UTILS.fromJson(
                            json,
                            TREE_MAP_STRING_STRING_TYPE,
                            e -> {
                                LOG.warn("VID:{} REDIS DB:{} KEY:{} 中不是合法json通知 {}", vehicleId, redisDb, noticeKey, json);
                                return null;
                            }),
                        Maps::newTreeMap)
                );
            } else {
                return ImmutableMap.of();
            }
        });
        return result;
    }

    @NotNull
    protected ImmutableMap<String, String> readMemoryVehicleNotice(@NotNull final String vehicleId) {
        return ObjectExtension.defaultIfNull(
            vehicleNoticeCache.get(vehicleId),
            ImmutableMap::of
        );
    }

    /**
     * 开始阈值连接帧数
     *
     * @return
     */
    protected int getBeginTriggerContinueCount() {
        return beginTriggerContinueCount;
    }

    /**
     * 开始阈值连续时长
     *
     * @return
     */
    protected long getBeginTriggerTimeoutMillisecond() {
        return beginTriggerTimeoutMillisecond;
    }

    /**
     * 结束阈值连接帧数
     *
     * @return
     */
    protected int getEndTriggerContinueCount() {
        return endTriggerContinueCount;
    }

    /**
     * 结束阈值连续时长
     *
     * @return
     */
    protected long getEndTriggerTimeoutMillisecond() {
        return endTriggerTimeoutMillisecond;
    }

    //endregion

    //region 内部处理方法

    /**
     * 初始化
     *
     * @param beginTriggerContinueCount      开始阈值
     * @param beginTriggerTimeoutMillisecond 开始阈值连续时长
     * @param endTriggerContinueCount        结束阈值
     * @param endTriggerTimeoutMillisecond   结束阈值连续时长
     */
    private void init(int beginTriggerContinueCount, long beginTriggerTimeoutMillisecond, int endTriggerContinueCount, long endTriggerTimeoutMillisecond) {
        this.beginTriggerContinueCount = beginTriggerContinueCount;
        this.beginTriggerTimeoutMillisecond = beginTriggerTimeoutMillisecond;
        this.endTriggerContinueCount = endTriggerContinueCount;
        this.endTriggerTimeoutMillisecond = endTriggerTimeoutMillisecond;
    }

    /**
     * 获取车辆状态
     *
     * @param vehicleId 车辆ID
     * @return
     */
    @NotNull
    private MultiDelaySwitch<State> ensureVehicleStatus(@NotNull final String vehicleId) {
        return vehicleStatus.computeIfAbsent(
            vehicleId,
            k -> {
                final ImmutableMap<String, String> startNotice = readRedisVehicleNotice(vehicleId);
                if (!startNotice.isEmpty()) {
                    final String status = startNotice.get(NOTICE_STATUS_KEY);
                    if (NOTICE_START_STATUS.equals(status)) {
                        return buildDelaySwitch().setSwitchStatus(State.BEGIN);
                    } else if (NOTICE_END_STATUS.equals(status)) {
                        LOG.warn("VID:{} REDIS DB:{} KEY:{} 中已结束的报警通知 {}", vehicleId, redisDb, buildRedisKey(), JSON_UTILS.toJson(startNotice));
                        removeNoticeToRedis(vehicleId);
                    } else {
                        LOG.warn("VID:{} REDIS DB:{} KEY:{} 中状态为 {} 的异常报警通知 {}", vehicleId, redisDb, buildRedisKey(), status, JSON_UTILS.toJson(startNotice));
                        removeNoticeToRedis(vehicleId);
                    }
                }
                return buildDelaySwitch().setSwitchStatus(State.END);
            }
        );
    }

    /**
     * 构建延迟开关
     *
     * @return
     */
    @NotNull
    @Contract(" -> new")
    private MultiDelaySwitch<State> buildDelaySwitch() {
        return new MultiDelaySwitch<State>()
            .setThresholdTimes(State.BEGIN, this.beginTriggerContinueCount)
            .setTimeoutMillisecond(State.BEGIN, this.beginTriggerTimeoutMillisecond)
            .setThresholdTimes(State.END, this.endTriggerContinueCount)
            .setTimeoutMillisecond(State.END, this.endTriggerTimeoutMillisecond);
    }

    /**
     * 删除redis中的车辆通知
     *
     * @param vehicleId 车辆ID
     */
    private void removeNoticeToRedis(@NotNull final String vehicleId) {
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(redisDb);
            jedis.hdel(buildRedisKey(), vehicleId);
        });
    }

    /**
     * 往redis写入车辆通知
     *
     * @param vehicleId 车辆ID
     */
    private void insertNoticeToRedis(@NotNull final String vehicleId, @NotNull final String value) {
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(redisDb);
            jedis.hset(buildRedisKey(), vehicleId, value);
        });
    }

    //endregion

    //region 需要子类实现的部分

    /**
     * 初始化redis key
     *
     * @return
     */
    protected abstract String buildRedisKey();

    /**
     * 处理之前
     * @param data 数据集
     */
    protected void beforeProcess(@NotNull final ImmutableMap<String, String> data) {
        // default do nothing
    }

    /**
     * 数据有效性过滤
     *
     * @param data
     * @return true 不处理该帧数据
     */
    protected boolean ignore(ImmutableMap<String, String> data) {
        //nothing to do.
        return false;
    }

    /**
     * 解析状态
     *
     * @param data
     * @return
     */
    protected abstract State parseState(ImmutableMap<String, String> data);

    /**
     * 初始化开始通知
     *
     * @param data                       车辆实时数据
     * @param vehicleId                  车辆ID
     * @param platformReceiverTimeString 平台接收时间
     * @return
     */
    @NotNull
    protected abstract ImmutableMap<String, String> initBeginNotice(
        @NotNull ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString);

    /**
     * 发送开始通知
     *
     * @param data      车辆实时数据
     * @param count     达到指定帧数
     * @param timeout   指定时间内
     * @param vehicleId 车辆VID
     * @return
     */
    protected abstract Map<String, String> buildBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        int count, long timeout,
        @NotNull String vehicleId);

    //region 结束通知不强制实现，根据需要重写这两个方法

    /**
     * 初始化结束通知
     *
     * @param data                       车辆实时数据
     * @param vehicleId                  车辆ID
     * @param platformReceiverTimeString 平台接收时间
     * @return
     */
    @NotNull
    protected ImmutableMap<String, String> initEndNotice(
        @NotNull ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        //nothing to do.
        return ImmutableMap.of();
    }

    /**
     * 发送结束通知
     *
     * @param data      车辆实时数据
     * @param count     达到指定帧数
     * @param timeout   指定时间内
     * @param vehicleId 车辆VID
     * @return
     */
    protected Map<String, String> buildEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        int count,
        long timeout,
        @NotNull String vehicleId) {
        //nothing to do.
        return ImmutableMap.of();
    }
    //endregion

    /**
     * 处理之后
     * @param data 数据集
     */
    protected void afterProcess(@NotNull final ImmutableMap<String, String> data) {
        // default do nothing
    }

    //endregion

    protected enum State {

        /**
         * 未知
         */
        UNKNOWN,

        /**
         * 开始
         */
        BEGIN,

        /**
         * 持续
         */
        CONTINUE,

        /**
         * 结束
         */
        END,
    }
}
