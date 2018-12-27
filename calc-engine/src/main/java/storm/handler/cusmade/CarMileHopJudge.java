package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import storm.extension.ObjectExtension;
import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.ConfigUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CarMileHopJudge {
    private static final Logger LOG = LoggerFactory.getLogger(CarMileHopJudge.class);
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();
    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();
    static int db = 6;
    static String mileHopRedisKeys = "vehCache.qy.mileHop.timeAndLastMileage";
    /**
     * 缓存里程跳变的最近一帧有效里程的里程和时间
     */
    private Map<String, Map<String, String>> vidMileHopCache = new HashMap<>();
    /**
     * 里程跳变处理（这块只是连续里程跳变处理）
     * @param data 实时报文
     * @return 实时里程跳变通知notice，json字符串类型。
     */
    public String processFrame(Map<String, String> data){

        if (MapUtils.isEmpty(data)) {
            return null;
        }
        try {
            final String vid = data.get(DataKey.VEHICLE_ID);
            final String time = data.get(DataKey.TIME);
            final String msgType = data.get(DataKey.MESSAGE_TYPE);
            if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(time)
                    || StringUtils.isEmpty(msgType)) {
                return null;
            }
            //判断是否为实时报文
            if (!CommandType.SUBMIT_REALTIME.equals(msgType)) {
                return null;
            }

            String nowMileage = data.get(DataKey._2202_TOTAL_MILEAGE);
            nowMileage = NumberUtils.isNumber(nowMileage) ? nowMileage : "0";

            //如果当前帧里程值无效，则返回null
            if (StringUtils.equals("0",nowMileage)){
                return null;
            }

            //如果缓存中没有这辆车的缓存，则去redis读一次。
            if (!vidMileHopCache.containsKey(vid)){
                JEDIS_POOL_UTILS.useResource(jedis -> {
                    jedis.select(db);
                    vidMileHopCache.put(vid,loadMileHopCacheFromRedis(jedis,vid));
                });
            }

            String mileHopNotice = null;
            //如果缓存中这辆车的缓存不为空，则往下操作
            if (!vidMileHopCache.get(vid).isEmpty()){
                //如果最后一帧里程值为无效，则返回null
                Map<String,String> lastUsefulMileCache = vidMileHopCache.get(vid);
                String lastMileage = String.valueOf(lastUsefulMileCache.get("lastUsefulMileage"));
                if (!StringUtils.equals("-1",lastMileage)){

                    //获得当前里程值、上一帧有效里程值、里程跳变值、里程跳变报警阈值。
                    int nowMile = Integer.parseInt(nowMileage);
                    int lastMile = Integer.parseInt(lastMileage);
                    int mileHopValue = Math.abs(nowMile - lastMile);
                    int mileHopThreshold = ConfigUtils.getSysDefine().getMileHopNum() * 10;
                    //如果里程差值小于里程跳变报警阈值则返回null
                    if (mileHopValue >= mileHopThreshold){
                        LOG.info("vid:{} 判定为里程跳变！从{}跳变为{}！", vid, lastMile, nowMile);
                        //发出里程跳变通知
                        String lastTime = String.valueOf(lastUsefulMileCache.get("time"));
                        String vin = data.get(DataKey.VEHICLE_NUMBER);
                        Map<String, String> mileageHopNotice = new TreeMap<>();
                        mileageHopNotice.put("msgType", NoticeType.HOP_MILE);
                        mileageHopNotice.put("vid", vid);
                        mileageHopNotice.put("vin", vin);
                        mileageHopNotice.put("stime", lastTime);
                        mileageHopNotice.put("etime", time);
                        mileageHopNotice.put("stmile", String.valueOf(lastMile));
                        mileageHopNotice.put("edmile", String.valueOf(nowMile));
                        mileageHopNotice.put("hopValue", String.valueOf(mileHopValue));
                        mileHopNotice = JSON_UTILS.toJson(mileageHopNotice);
                    }
                }
            }
            saveNowTimeAndMileage(vid,time,nowMileage);
            return mileHopNotice;
        } catch (Exception e) {
            LOG.error("VID:{},里程跳变判断发生异常，异常信息如下：{}",data.get(DataKey.VEHICLE_ID),e);
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 从redis中加载为计算里程跳变所存储的最近一帧有效里程值
     * 可以将redis中缓存的json字符串转化为map形式
     * @param jedis
     * @param vehicleId 车辆id
     * @return map形式的最近一帧有效里程值缓存
     */
    @NotNull
    private ImmutableMap<String, String> loadMileHopCacheFromRedis(
            @NotNull final Jedis jedis,
            @NotNull final String vehicleId) {

        final String json = jedis.hget(mileHopRedisKeys, vehicleId);
        if (StringUtils.isNotBlank(json)) {
            return ImmutableMap.copyOf(
                    ObjectExtension.defaultIfNull(
                            JSON_UTILS.fromJson(
                                    json,
                                    TREE_MAP_STRING_STRING_TYPE,
                                    e -> {
                                        LOG.warn("VID:{} REDIS DB:{} KEY:{} 中不是合法json的里程跳变的时间与里程值缓存{}", vehicleId, db, mileHopRedisKeys, json);
                                        return null;
                                    }),
                            Maps::newTreeMap)
            );
        } else {
            return ImmutableMap.of();
        }
    }

    /**
     * 保存“时间”和“里程值”到缓存和redis中
     * @param vid 车辆id
     * @param time 最近一帧有效里程值的服务器接收报文时间
     * @param nowMileage 最近一帧有效里程值
     */
    private void saveNowTimeAndMileage(String vid, String time, String nowMileage){
        Map<String, String> usefulTimeAndMileage = new HashMap<>();
        usefulTimeAndMileage.put("time",time);
        usefulTimeAndMileage.put("lastUsefulMileage",nowMileage);
        vidMileHopCache.put(vid,usefulTimeAndMileage);

        final String json = JSON_UTILS.toJson(usefulTimeAndMileage);
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(db);
            jedis.hset(mileHopRedisKeys, vid, json);
        });
    }

}
