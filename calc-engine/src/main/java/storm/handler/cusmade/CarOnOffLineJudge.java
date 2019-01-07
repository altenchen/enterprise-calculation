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
import storm.util.ConfigUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.lang.reflect.Type;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

/**
 * @author yuxinzhao
 */
public class CarOnOffLineJudge {
    private static final Logger LOG = LoggerFactory.getLogger(CarOnOffLineJudge.class);
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();
    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();
    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();
    private static int REDIS_DB_INDEX = 6;
    private static String onOffMileRedisKeys = "vehCache.qy.onOffMile.notice";
    /**
     * 缓存车辆下线时的里程值通知，以便在车辆上线时发出车辆上下线里程通知。
     */
    private Map<String, Map<String, String>> vidOnOffMileNotice = new HashMap<>();

    /**
     * 上下线里程通知处理（这块只是再次上线里程跳变的处理，只是发出上线和下线时的里程值通知，具体是否判断为上下线里程跳变逻辑由kafkaservice判断，
     * 感觉可以优化，将计算 逻辑都放到storm中来。）
     *
     * @param realTimeData 实时报文
     * @return 上下线时的里程通知notice，格式为json字符串类型。
     */
    public String processFrame(final ImmutableMap<String, String> realTimeData) {

        if (MapUtils.isEmpty(realTimeData)) {
            return null;
        }

        try {
            final String vid = realTimeData.get(DataKey.VEHICLE_ID);
            final String time = realTimeData.get(DataKey.TIME);
            final String msgType = realTimeData.get(DataKey.MESSAGE_TYPE);
            if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(time)
                    || StringUtils.isEmpty(msgType)) {
                return null;
            }

            final long currentTimeMillis = System.currentTimeMillis();
            final String nowTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

            String nowMileage = realTimeData.get(DataKey._2202_TOTAL_MILEAGE);
            nowMileage = NumberUtils.isNumber(nowMileage) ? nowMileage : "0";

            //如果当前帧里程值无效，则返回将最近一帧有效里程作为当前里程。
            if (StringUtils.equals("0", nowMileage)) {
                nowMileage = VEHICLE_CACHE.getTotalMileageString(vid, "0");
            }

            String onoffMlieNotice = null;
            //如果缓存中没有这辆车的缓存，则去redis读一次。
            if (!vidOnOffMileNotice.containsKey(vid)) {
                JEDIS_POOL_UTILS.useResource(jedis -> {
                    jedis.select(REDIS_DB_INDEX);
                    vidOnOffMileNotice.put(vid, loadOnOffMlieNoticeFromRedis(jedis, vid));
                });
            }

            boolean isOffLine = isOffLineByRealTimeData(realTimeData);

            if (isOffLine) {
                if (!vidOnOffMileNotice.containsKey(vid)) {
                    Map<String, String> notice = new HashMap<>();
                    notice.put("msgType", NoticeType.ON_OFF_MILE);
                    notice.put("vid", vid);
                    notice.put("stime", time);
                    notice.put("smileage", nowMileage);
                    vidOnOffMileNotice.put(vid, notice);
                    //保存开始通知到redis中
                    onoffMlieNotice = JSON_UTILS.toJson(notice);
                    saveOnOffMlieNotice(vid,onoffMlieNotice);
                }
            } else {
                if (CommandType.SUBMIT_REALTIME.equals(msgType)){
                    if (vidOnOffMileNotice.containsKey(vid)) {
                        Map<String, String> notice = vidOnOffMileNotice.get(vid);
                        if (null != notice) {
                            notice.put("etime", time);
                            notice.put("emileage", nowMileage);
                            notice.put("noticetime", nowTime);

                            onoffMlieNotice = JSON_UTILS.toJson(notice);
                            vidOnOffMileNotice.remove(vid);
                            deleteOnOffMlieNotice(vid);

                            return onoffMlieNotice;
                        }
                    }
                }
            }


        } catch (Exception e) {
            LOG.error("VID:{},上下线里程通知判断发生异常，异常信息如下：{}", realTimeData.get(DataKey.VEHICLE_ID), e);
            e.printStackTrace();
        }

        return null;
    }


    private void saveOnOffMlieNotice(final String vid, final String onoffMlieNotice){
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            jedis.hset(onOffMileRedisKeys, vid, onoffMlieNotice);
        });
    }

    private void deleteOnOffMlieNotice(final String vid){
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            jedis.hdel(onOffMileRedisKeys, vid);
        });
    }
    /**
     * 从redis中加载为计算上下线里程所存储的上下线里程开始通知
     * 可以将redis中缓存的json字符串转化为map形式
     *
     * @param jedis
     * @param vehicleId 车辆id
     * @return map形式的上下线里程开始通知
     */
    @NotNull
    private ImmutableMap<String, String> loadOnOffMlieNoticeFromRedis(
            @NotNull final Jedis jedis,
            @NotNull final String vehicleId) {

        final String json = jedis.hget(onOffMileRedisKeys, vehicleId);
        if (StringUtils.isNotBlank(json)) {
            return ImmutableMap.copyOf(
                    ObjectExtension.defaultIfNull(
                            JSON_UTILS.fromJson(
                                    json,
                                    TREE_MAP_STRING_STRING_TYPE,
                                    e -> {
                                        LOG.warn("VID:{} REDIS DB:{} KEY:{} 中不是合法json的上下线里程通知{}", vehicleId, REDIS_DB_INDEX, onOffMileRedisKeys, json);
                                        return null;
                                    }),
                            Maps::newTreeMap)
            );
        } else {
            return ImmutableMap.of();
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

        //1、登入登出报文，根据平台登入注册类型和登入登出流水号判断。
        if (CommandType.SUBMIT_LOGIN.equals(msgType)) {

            final String logoutSeq = realTimeData.get(SUBMIT_LOGIN.LOGOUT_SEQ);
            final String loginSeq = realTimeData.get(SUBMIT_LOGIN.LOGIN_SEQ);
            final String regType = realTimeData.get(ProtocolItem.REG_TYPE);

            //1.1、先根据自带的TYPE字段进行判断。
            // 平台注册通知类型 0:从未上过线，1:车机终端上线 ，2:车机离线，3:平台上线，4:平台下线
            if ("2".equals(regType)) {
                return true;
            }

            //1.2、如果自带的type字段没数据，则根据登入登出流水号判断。
            if (!StringUtils.isEmpty(logoutSeq) && !StringUtils.isEmpty(logoutSeq)) {
                int logout = Integer.parseInt(NumberUtils.isNumber(logoutSeq) ? logoutSeq : "0");
                int login = Integer.parseInt(NumberUtils.isNumber(loginSeq) ? loginSeq : "0");
                if (logout > login) {
                    return true;
                }
            }
        }

        //2、如果是链接状态报文，则根据连接状态字段进行判断，1上线，2心跳，3离线
        if (CommandType.SUBMIT_LINKSTATUS.equals(msgType)) {
            final String linkType = realTimeData.get(SUBMIT_LINKSTATUS.LINK_TYPE);
            if ("3".equals(linkType)){
                return true;
            }
        }

        return false;
    }


//    /**
//     * 判断车辆是否离线，最本质的判断离线方法，根据报文不同类型采用不同方法判断
//     * @param realTimeData
//     * @return 是否离线
//     */
//
//    private boolean isOffLineByRealTimeData(Map<String, String> realTimeData){
//        final String msgType = realTimeData.get(DataKey.MESSAGE_TYPE);
//
//        //1、实时报文，返回false
//        if (CommandType.SUBMIT_REALTIME.equals(msgType)){
//            return false;
//        }
//
//        //2、登入登出报文，根据平台登入注册类型和登入登出流水号判断。
//        if (CommandType.SUBMIT_LOGIN.equals(msgType)) {
//            //2.1、先根据自带的TYPE字段进行判断。
//            // 平台注册通知类型 0:从未上过线，1:车机终端上线 ，2:车机离线，3:平台上线，4:平台下线
//            final String type = realTimeData.get(ProtocolItem.REG_TYPE);
//            switch (type){
//                case "1":
//                    return false;
//                case "2":
//                    return true;
//                default:
//                    break;
//            }
//            //2.2、如果自带的type字段没数据，则根据登入登出流水号判断。
//            final String logoutSeq = realTimeData.get(SUBMIT_LOGIN.LOGOUT_SEQ);
//            final String loginSeq = realTimeData.get(SUBMIT_LOGIN.LOGIN_SEQ);
//
//            if (!StringUtils.isEmpty(logoutSeq) && !StringUtils.isEmpty(logoutSeq)) {
//                int logout = Integer.parseInt(NumberUtils.isNumber(logoutSeq) ? logoutSeq : "0");
//                int login = Integer.parseInt(NumberUtils.isNumber(loginSeq) ? loginSeq : "0");
//                if(login >logout){
//                    return false;
//                }
//            }
//
//            if (StringUtils.isEmpty(loginSeq)) {
//                return false;
//            }
//            return true;
//        }
//
//        //3、如果是链接状态报文，则根据连接状态字段进行判断，1上线，2心跳，3离线
//        if (CommandType.SUBMIT_LINKSTATUS.equals(msgType)){
//            final String linkType = realTimeData.get(SUBMIT_LINKSTATUS.LINK_TYPE);
//            switch (linkType){
//                case "1":
//                    return false;
//                case "2":
//                    return false;
//                case "3":
//                    return true;
//                default:
//                    break;
//            }
//        }
//
//        return false;
//    }

}
