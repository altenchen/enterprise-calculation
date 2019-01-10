package storm.handler.cusmade;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dto.notice.VehicleNotice;
import storm.extension.ObjectExtension;
import storm.system.DataKey;
import storm.tool.MultiDelaySwitch;
import storm.util.DataUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.lang.reflect.ParameterizedType;
import java.text.ParseException;
import java.util.Map;

/**
 * 实现了车辆基本的连续帧数统计处理
 * 持久化状态存储 内存 <==> redis
 *
 * @author 智杰
 */
public abstract class AbstractVehicleDelaySwitchJudge<E extends VehicleNotice> {

    //region 其他属性

    private static final Logger LOG = LoggerFactory.getLogger(AbstractVehicleDelaySwitchJudge.class);

    /**
     * 通知类的class
     */
    private Class<E> noticeClass;

    //endregion

    //region redis操作相关属性

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    /**
     * 默认存储在6库
     */
    private final int redisDb;

    private static final String NOTICE_START_STATUS = "1";

    private static final String NOTICE_END_STATUS = "3";

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
     * 车辆状态缓存表 <vehicleId, 车辆状态>
     */
    private final Map<String, MultiDelaySwitch<NoticeState>> vehicleStatus = Maps.newHashMap();

    /**
     * 车辆开始通知缓存表 <vehicleId, 车辆通知>
     */
    private final Map<String, E> vehicleBeginNoticeCache = Maps.newHashMap();

    /**
     * 车辆临时通知缓存表 <vehicleId, 车辆通知>
     * 主要存储：开始通知的首帧初始化， 结束通知的首帧初始化
     */
    private final Map<String, E> vehicleTempNoticeCache = Maps.newHashMap();

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

    /**
     * 处理实时数据
     *
     * @param data 车辆实时数据
     * @return
     */
    @Nullable
    public String processFrame(@NotNull final ImmutableMap<String, String> data) {
        beforeProcess(data);
        String result = handle(data);
        afterProcess(data);
        return result;
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
                .setThresholdTimes(NoticeState.BEGIN, this.beginTriggerContinueCount)
                .setTimeoutMillisecond(NoticeState.BEGIN, this.beginTriggerTimeoutMillisecond)
                .setThresholdTimes(NoticeState.END, this.endTriggerContinueCount)
                .setTimeoutMillisecond(NoticeState.END, this.endTriggerTimeoutMillisecond);
        });
    }

    @NotNull
    public NoticeState getState(@NotNull final String vehicleId) {
        return ObjectExtension.defaultIfNull(
            ensureVehicleStatus(vehicleId).getSwitchStatus(),
            NoticeState.UNKNOWN
        );
    }
    //endregion

    //region 供子类调用

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

    /**
     * 删除redis缓存
     *
     * @param redisKey
     * @param vehicleId
     */
    protected void removeRedisCache(String redisKey, String vehicleId) {
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(redisDb);
            jedis.hdel(redisKey, vehicleId);
        });
    }

    /**
     * 写入redis缓存
     *
     * @param redisKey
     * @param vehicleId
     * @param vlaue
     */
    protected void writeRedisCache(String redisKey, String vehicleId, String vlaue) {
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(redisDb);
            jedis.hset(redisKey, vehicleId, vlaue);
        });
    }

    /**
     * 读取redis缓存
     *
     * @param redisKey
     * @param vehicleId
     */
    protected String readRedisCache(String redisKey, String vehicleId) {
        return JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(redisDb);
            return jedis.hget(redisKey, vehicleId);
        });
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
     * 处理车辆实时数据
     *
     * @param data 车辆实时数据
     * @return
     */
    @Nullable
    private String handle(@NotNull final ImmutableMap<String, String> data) {
        final String vehicleId = data.get(DataKey.VEHICLE_ID);
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
                        data,
                        platformReceiverTime,
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

    /**
     * 获取车辆状态
     *
     * @param vehicleId 车辆ID
     * @return
     */
    @NotNull
    private MultiDelaySwitch<NoticeState> ensureVehicleStatus(@NotNull final String vehicleId) {
        return vehicleStatus.computeIfAbsent(
            vehicleId,
            k -> {
                final E startNotice = readRedisVehicleNotice(vehicleId);
                if (startNotice != null) {
                    final String status = startNotice.getStatus();
                    if (NOTICE_START_STATUS.equals(status)) {
                        return buildDelaySwitch().setSwitchStatus(NoticeState.BEGIN);
                    } else if (NOTICE_END_STATUS.equals(status)) {
                        LOG.warn("VID:{} REDIS DB:{} KEY:{} 中已结束的报警通知 {}", vehicleId, redisDb, buildRedisKey(), JSON_UTILS.toJson(startNotice));
                        removeRedisNotice(vehicleId);
                    } else {
                        LOG.warn("VID:{} REDIS DB:{} KEY:{} 中状态为 {} 的异常报警通知 {}", vehicleId, redisDb, buildRedisKey(), status, JSON_UTILS.toJson(startNotice));
                        removeRedisNotice(vehicleId);
                    }
                }
                return buildDelaySwitch().setSwitchStatus(NoticeState.END);
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
    private MultiDelaySwitch<NoticeState> buildDelaySwitch() {
        return new MultiDelaySwitch<NoticeState>()
            .setThresholdTimes(NoticeState.BEGIN, this.beginTriggerContinueCount)
            .setTimeoutMillisecond(NoticeState.BEGIN, this.beginTriggerTimeoutMillisecond)
            .setThresholdTimes(NoticeState.END, this.endTriggerContinueCount)
            .setTimeoutMillisecond(NoticeState.END, this.endTriggerTimeoutMillisecond);
    }

    /**
     * 删除redis中的车辆通知
     *
     * @param vehicleId 车辆ID
     */
    private void removeRedisNotice(@NotNull final String vehicleId) {
        removeRedisCache(buildRedisKey(), vehicleId);
    }

    /**
     * 查询车辆开始通知
     * 内存中没有车辆通知，则从redis查找
     *
     * @param vehicleId 车辆ID
     */
    private E queryBeginNotice(@NotNull final String vehicleId) {
        if (vehicleBeginNoticeCache.containsKey(vehicleId)) {
            //返回内存中的车辆通知
            return vehicleBeginNoticeCache.get(vehicleId);
        }
        //从redis读取
        E redisBeginNotice = readRedisVehicleNotice(vehicleId);
        vehicleBeginNoticeCache.put(vehicleId, redisBeginNotice);
        return redisBeginNotice;
    }

    /**
     * 读取redis中的通知
     *
     * @param vehicleId 车辆ID
     * @return
     */
    private E readRedisVehicleNotice(@NotNull final String vehicleId) {
        String json = readRedisCache(buildRedisKey(), vehicleId);
        if (StringUtils.isEmpty(json)) {
            return null;
        }
        return JSON.parseObject(json, getNoticeClass());
    }

    /**
     * 处理开始通知
     *
     * @param vehicleId                  车辆ID
     * @param data                       车辆实时数据
     * @param platformReceiverTime       平台接收时间
     * @param platformReceiverTimeString 平台接收时间
     * @return
     */
    @Nullable
    private String processBeginFrame(
        final String vehicleId,
        final @NotNull ImmutableMap<String, String> data,
        final long platformReceiverTime,
        final String platformReceiverTimeString) {

        return ensureVehicleStatus(vehicleId).increase(
            NoticeState.BEGIN,
            platformReceiverTime,
            state -> {
                //初始化开始通知
                E notice = initBeginNotice(data, vehicleId, platformReceiverTimeString);
                vehicleTempNoticeCache.put(vehicleId, notice);
            },
            (state, count, timeout) -> {
                //构建开始通知
                E notice = vehicleTempNoticeCache.get(vehicleId);
                //将通知状态设置为 ==> 开始
                notice.setStatus(NOTICE_START_STATUS);
                buildBeginNotice(data, count, timeout, vehicleId, notice);
                //缓存开始通知
                vehicleBeginNoticeCache.put(vehicleId, notice);
                //删除车辆临时缓存
                vehicleTempNoticeCache.remove(vehicleId);
                //将车辆通知写入REDIS
                final String json = JSON_UTILS.toJson(notice);
                writeRedisCache(buildRedisKey(), vehicleId, json);
                return json;
            }
        );
    }

    /**
     * 处理结束通知
     *
     * @param vehicleId                  车辆ID
     * @param data                       车辆实时数据
     * @param platformReceiverTime       平台接收时间
     * @param platformReceiverTimeString 平台接收时间
     * @return
     */
    @Nullable
    private String processEndFrame(
        final String vehicleId,
        final @NotNull ImmutableMap<String, String> data,
        final long platformReceiverTime,
        final String platformReceiverTimeString) {

        return ensureVehicleStatus(vehicleId).increase(
            NoticeState.END,
            platformReceiverTime,
            state -> {
                if (!hasBeginNotice(vehicleId)) {
                    //没有触发过开始通知
                    return;
                }
                //初始化结束通知
                E endInitNotice = initEndNotice(data, vehicleId, platformReceiverTimeString);
                vehicleTempNoticeCache.put(vehicleId, endInitNotice);
            },
            (state, count, timeout) -> {
                if (!hasBeginNotice(vehicleId)) {
                    //没有触发过开始通知
                    return null;
                }
                E endInitNotice = vehicleTempNoticeCache.get(vehicleId);
                E beginNotice = queryBeginNotice(vehicleId);
                //将 endInitNotice(忽略null值) 覆盖到 beginNotice
                BeanUtil.copyProperties(endInitNotice, beginNotice, CopyOptions.create().setIgnoreNullValue(true));
                //将通知状态设置为 ==> 结束
                beginNotice.setStatus(NOTICE_END_STATUS);
                //构建结束通知
                buildEndNotice(data, count, timeout, vehicleId, beginNotice);
                //清除车辆开始通知
                removeRedisNotice(vehicleId);
                vehicleBeginNoticeCache.remove(vehicleId);
                //清除车辆临时通知
                vehicleTempNoticeCache.remove(vehicleId);
                return JSON_UTILS.toJson(beginNotice);
            }
        );
    }

    /**
     * 是否有触发过开始通知
     *
     * @return true 有， false 没有
     */
    private boolean hasBeginNotice(String vehicleId) {
        final E beginNotice = queryBeginNotice(vehicleId);
        if (beginNotice == null) {
            return false;
        }
        return true;
    }

    /**
     * 获取Notice的class
     *
     * @return
     */
    private synchronized Class<E> getNoticeClass() {
        if (noticeClass == null) {
            noticeClass = (Class<E>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        }
        return noticeClass;
    }

    //endregion

    //region 需要子类实现的部分

    /**
     * 初始化redis key用于持久化通知
     *
     * @return
     */
    protected abstract String buildRedisKey();

    /**
     * 处理之前
     *
     * @param data 车辆实时数据
     */
    protected void beforeProcess(final ImmutableMap<String, String> data) {
        // default do nothing
    }

    /**
     * 忽略无效数据
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
    protected abstract NoticeState parseState(ImmutableMap<String, String> data);

    /**
     * 初始化开始通知
     *
     * @param data                       车辆实时数据
     * @param vehicleId                  车辆ID
     * @param platformReceiverTimeString 平台接收时间
     * @return
     */
    @NotNull
    protected abstract E initBeginNotice(
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
     * @param notice    车辆通知
     */
    protected abstract void buildBeginNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final E notice);

    //region 结束通知不强制实现，根据需要重写这两个方法

    /**
     * 初始化结束通知
     *
     * @param data                       车辆实时数据
     * @param vehicleId                  车辆ID
     * @param platformReceiverTimeString 平台接收时间
     * @return
     */
    protected E initEndNotice(
        @NotNull ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString) {

        //nothing to do.
        return null;
    }

    /**
     * 发送结束通知
     *
     * @param data      车辆实时数据
     * @param count     达到指定帧数
     * @param timeout   指定时间内
     * @param vehicleId 车辆VID
     * @param notice    车辆通知
     * @return
     */
    protected void buildEndNotice(
        @NotNull final ImmutableMap<String, String> data,
        final int count,
        final long timeout,
        @NotNull String vehicleId,
        @NotNull final E notice) {
        //nothing to do.
    }
    //endregion

    /**
     * 处理之后
     *
     * @param data 车辆实时数据
     */
    protected void afterProcess(@NotNull final ImmutableMap<String, String> data) {
        // default do nothing
    }

    //endregion
}
