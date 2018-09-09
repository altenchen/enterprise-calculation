package storm.spout;

import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.Utils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import storm.constant.RedisConstant;
import storm.stream.MessageId;
import storm.stream.NoticeStream;
import storm.stream.StreamReceiverFilter;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-09-07
 * @description: 初始化缓存
 */
public final class IdleVehicleNoticeSpout extends BaseRichSpout {

    private static final long serialVersionUID = -2889995817929858336L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(IdleVehicleNoticeSpout.class);

    // region Component

    @NotNull
    private static final String COMPONENT_ID = IdleVehicleNoticeSpout.class.getSimpleName();

    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    // region NoticeStream

    @NotNull
    private static final NoticeStream NOTICE_STREAM = NoticeStream.getInstance();

    @NotNull
    private static final String NOTICE_STREAM_ID = NOTICE_STREAM.getStreamId(COMPONENT_ID);

    @NotNull
    @Contract(pure = true)
    public static String getNoticeStreamId() {
        return NOTICE_STREAM_ID;
    }

    @NotNull
    public static StreamReceiverFilter prepareNoticeStreamReceiver(
        @NotNull final NoticeStream.IProcessor processor) {

        return NOTICE_STREAM
            .prepareReceiver(
                processor)
            .filter(
                COMPONENT_ID,
                NOTICE_STREAM_ID);
    }

    // endregion NoticeStream

    // region 静态常量

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    private static final int REDIS_DATABASE_INDEX = 6;

    private static final String IDLE_VEHICLE_REDIS_KEY = "vehCache.qy.idle";

    private static final long POLL_INTERVAL_IN_MILLISECONDS = TimeUnit.MINUTES.toMillis(1);

    // endregion 静态常量

    private transient NoticeStream.SpoutSender noticeStreamSender;

    @Override
    public void open(
        @NotNull final Map conf,
        @NotNull final TopologyContext context,
        @NotNull final SpoutOutputCollector collector) {

        noticeStreamSender = NOTICE_STREAM.openSender(NOTICE_STREAM_ID, collector);
    }

    @Override
    public void nextTuple() {

        final Map<String, String> idleNotices = JEDIS_POOL_UTILS.useResource((Jedis jedis) -> {

            if(!RedisConstant.Select.OK.equals(jedis.select(REDIS_DATABASE_INDEX))) {
                LOG.error("切换到 redis [{}]库失败.");
                return ImmutableMap.of();
            }

            return jedis.hgetAll(IDLE_VEHICLE_REDIS_KEY);
        });

        if(MapUtils.isNotEmpty(idleNotices)) {

            idleNotices.forEach((vid, json) ->{

                try {

                    final TreeMap<String, String> notice = JSON_UTILS.fromJson(
                        json,
                        TREE_MAP_STRING_STRING_TYPE);

                    if(MapUtils.isNotEmpty(notice)) {

                        noticeStreamSender.emit(
                            new MessageId<>(vid),
                            vid,
                            ImmutableMap.copyOf(notice));
                    }

                } catch (Exception e) {
                    LOG.warn("解析闲置车辆通知异常", e);
                    LOG.warn("解析闲置车辆通知异常[{}][{}]", vid, json);
                }
            });
        }

        Utils.sleep(POLL_INTERVAL_IN_MILLISECONDS);
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {

        NOTICE_STREAM.declareOutputFields(NOTICE_STREAM_ID, declarer);
    }
}
