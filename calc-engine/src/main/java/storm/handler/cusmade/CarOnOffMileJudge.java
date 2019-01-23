package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import storm.cache.VehicleCache;
import storm.constant.FormatConstant;
import storm.extension.ObjectExtension;
import storm.protocol.CommandType;
import storm.protocol.SUBMIT_LINKSTATUS;
import storm.protocol.SUBMIT_LOGIN;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.system.ProtocolItem;
import storm.util.DataUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author yuxinzhao
 */
public class CarOnOffMileJudge {
    private static final Logger LOG = LoggerFactory.getLogger(CarOnOffMileJudge.class);
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();
    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();
    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();
    private static int REDIS_DB_INDEX = 6;
    private static String ONOFFMILE_REDISKEY = "vehCache.qy.onOffMile.notice";
    /**
     * 如果实时数据中的里程值不存在，则赋值为默认值0。
     */
    private static String MILEAGE_DEFAULT = "0";
    /**
     * 缓存车辆下线时的里程值通知，以便在车辆上线时发出车辆上下线里程通知。
     */
    private Map<String, Map<String, String>> vidOnOffMileNotice = new HashMap<>(16);

    /**
     * 上下线里程通知处理（当车辆再次上线时发出上下线里程通知）
     * （这块只是“再次上线里程跳变”的处理，只是发出上线和下线时的里程值通知，
     * 具体是否判断为上下线里程跳变逻辑由kafkaservice判断，感觉可以优化，将计算 逻辑都放到storm中来。）
     *
     * @param realTimeData 实时报文（topic为us_general的报文，里面的报文类型不止“REALTIME”一种）
     * @return 上下线时的里程通知notice，格式为json字符串类型。
     */
    public String processFrame(final ImmutableMap<String, String> realTimeData) {
        if (MapUtils.isEmpty(realTimeData)) {
            return null;
        }

        final String vid = realTimeData.get(DataKey.VEHICLE_ID);
        final String time = realTimeData.get(DataKey.TIME);
        final String msgType = realTimeData.get(DataKey.MESSAGE_TYPE);
        if (StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)
                || StringUtils.isEmpty(msgType)) {
            return null;
        }

        try {
            String nowMileage = realTimeData.get(DataKey._2202_TOTAL_MILEAGE);
            nowMileage = NumberUtils.isNumber(nowMileage) ? nowMileage : MILEAGE_DEFAULT;

            //如果当前帧里程值无效，则将最近一帧有效里程作为当前里程。
            if (StringUtils.equals(MILEAGE_DEFAULT, nowMileage)) {
                nowMileage = VEHICLE_CACHE.getTotalMileageString(vid, MILEAGE_DEFAULT);
            }

            //如果缓存中没有这辆车的缓存，则去redis读一次。
            if (!vidOnOffMileNotice.containsKey(vid)) {
                JEDIS_POOL_UTILS.useResource(jedis -> {
                    jedis.select(REDIS_DB_INDEX);
                    vidOnOffMileNotice.put(vid, loadOnOffMileNoticeFromRedis(jedis, vid));
                });
            }

            boolean isEmptyOnOffMileNoticeCache = vidOnOffMileNotice.get(vid).isEmpty();

            boolean isOffLine = isOffLineByRealTimeData(realTimeData);
            String onOffMileNotice;
            //车辆离线  并且  没有缓存上下线里程开始通知
            if (isOffLine && isEmptyOnOffMileNoticeCache) {
                Map<String, String> notice = new HashMap<>(16);
                notice.put("msgType", NoticeType.ON_OFF_MILE);
                notice.put("vid", vid);
                notice.put("stime", time);
                notice.put("smileage", nowMileage);
                vidOnOffMileNotice.put(vid, notice);

                onOffMileNotice = JSON_UTILS.toJson(notice);
                saveOnOffMileNotice(vid, onOffMileNotice);
            }
            //车辆在线  并且  缓存了上下线里程开始通知 并且 此报文是实时报文类型（"REALTIME"）
            if (!isOffLine && !isEmptyOnOffMileNoticeCache && CommandType.SUBMIT_REALTIME.equals(msgType)) {

                final long currentTimeMillis = System.currentTimeMillis();
                final String nowTime = DataUtils.buildFormatTime(currentTimeMillis);

                Map<String, String> notice = vidOnOffMileNotice.get(vid);
                notice.put("etime", time);
                notice.put("emileage", nowMileage);
                notice.put("noticetime", nowTime);
                onOffMileNotice = JSON_UTILS.toJson(notice);

                vidOnOffMileNotice.remove(vid);
                deleteOnOffMileNotice(vid);
                return onOffMileNotice;
            }
        } catch (Exception e) {
            LOG.error("VID:" + realTimeData.get(DataKey.VEHICLE_ID) + ",上下线里程通知判断发生异常，异常信息如下：{}", e);
        }
        return null;
    }


    /**
     * 保存上下线里程通知的开始通知到redis中。
     *
     * @param vid
     * @param onoffMileNotice
     */
    private void saveOnOffMileNotice(final String vid, final String onoffMileNotice) {
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            jedis.hset(ONOFFMILE_REDISKEY, vid, onoffMileNotice);
        });
    }

    /**
     * 删除redis中上下线里程通知的开始通知
     *
     * @param vid
     */
    private void deleteOnOffMileNotice(final String vid) {
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            jedis.hdel(ONOFFMILE_REDISKEY, vid);
        });
    }

    /**
     * 从redis中加载为计算上下线里程所存储的上下线里程开始通知
     * 可以将redis中缓存的json字符串转化为map形式
     * 返回值一定不为null，如果reids中也没有，则返回《vid，map（不含数据的map，但并不为null）》
     *
     * @param jedis
     * @param vehicleId 车辆id
     * @return map形式的上下线里程开始通知
     */
    @NotNull
    private Map<String, String> loadOnOffMileNoticeFromRedis(@NotNull final Jedis jedis, @NotNull final String vehicleId) {

        final String json = jedis.hget(ONOFFMILE_REDISKEY, vehicleId);
        if (StringUtils.isNotBlank(json)) {
            return ObjectExtension.defaultIfNull(
                    JSON_UTILS.fromJson(
                            json,
                            TREE_MAP_STRING_STRING_TYPE,
                            e -> {
                                LOG.warn("VID:{} REDIS DB:{} KEY:{} 中不是合法json的上下线里程通知{}", vehicleId, REDIS_DB_INDEX, ONOFFMILE_REDISKEY, json);
                                return null;
                            }),
                    Maps::newTreeMap
            );
        } else {
            HashMap<String, String> map = Maps.newHashMap();
            return map;
        }
    }


    /**
     * 判断车辆是否离线，最本质的判断离线方法，根据报文不同类型采用不同方法判断
     *
     * @param realTimeData
     * @return 是否离线
     */
    private boolean isOffLineByRealTimeData(Map<String, String> realTimeData) {
        final String msgType = realTimeData.get(DataKey.MESSAGE_TYPE);

        switch (msgType){

             //1、登入登出报文，根据平台登入注册类型和登入登出流水号判断。
            case CommandType.SUBMIT_LOGIN:
                final String logoutSeq = realTimeData.get(SUBMIT_LOGIN.LOGOUT_SEQ);
                final String loginSeq = realTimeData.get(SUBMIT_LOGIN.LOGIN_SEQ);
                final String regType = realTimeData.get(ProtocolItem.REG_TYPE);

                //1.1、先根据自带的TYPE字段进行判断。
                // 平台注册通知类型 0:从未上过线，1:车机终端上线 ，2:车机离线，3:平台上线，4:平台下线
                if ("2".equals(regType)) {
                    LOG.info("vid:{} 根据登入报文中的Type字段判定为车辆离线！", realTimeData.get(DataKey.VEHICLE_ID));
                    return true;
                }

                //1.2、如果自带的type字段没数据，则根据登入登出流水号判断。
                if (!StringUtils.isEmpty(logoutSeq) && !StringUtils.isEmpty(logoutSeq)) {
                    int logout = Integer.parseInt(NumberUtils.isNumber(logoutSeq) ? logoutSeq : "0");
                    int login = Integer.parseInt(NumberUtils.isNumber(loginSeq) ? loginSeq : "0");
                    if (logout > login) {
                        LOG.info("vid:{} 根据登入报文中的登入登出流水号判定为车辆离线！", realTimeData.get(DataKey.VEHICLE_ID));
                        return true;
                    }
                }
                break;

            //2、如果是链接状态报文，则根据连接状态字段进行判断，1上线，2心跳，3离线
            case CommandType.SUBMIT_LINKSTATUS:
                final String linkType = realTimeData.get(SUBMIT_LINKSTATUS.LINK_TYPE);
                if ("3".equals(linkType)) {
                    LOG.info("vid:{} 根据链接状态报文中的连接状态字段判定为车辆离线！", realTimeData.get(DataKey.VEHICLE_ID));
                    return true;
                }
                break;

             default:
                 break;
        }
        return false;
    }

//    /**
//     * 判断车辆是否长时间没发报文了。（即如果很久没法报文了，也被判定为车辆离线）
//     * @param lastTime 车辆最后一帧有效数据中的时间
//     * @param currentTimeMillis 当前时间
//     * @param idleTimeout 判断车辆为离线的时间阈值
//     * @return
//     */
//    private boolean isTimeOut(final String lastTime, final long currentTimeMillis, final long idleTimeout){
//
//        long lastTimeValue = 0;
//        if(NumberUtils.isDigits(lastTime)) {
//            try {
//                lastTimeValue = DateUtils.parseDate(lastTime, new String[]{FormatConstant.DATE_FORMAT}).getTime();
//            } catch (ParseException e) {
//                LOG.warn("闲置时间是否超时判断中报出异常", e);
//            }
//        }
//
//        if (lastTimeValue > 0 && currentTimeMillis - lastTimeValue > idleTimeout) {
//            return true;
//        }
//        return false;
//    }

}
