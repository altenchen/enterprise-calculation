package storm.handler.cusmade;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import storm.dao.DataToRedis;
import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.NumberUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import storm.dao.DataToRedis;
import storm.handler.ctx.Recorder;
import storm.handler.ctx.RedisRecorder;

import static storm.handler.cusmade.CarRuleHandler.timeformat;

public class CarLockStatusChangeJudge {

    DataToRedis redis = new DataToRedis();
    public static final int db = 6;
    public static final String lockStatusRedisKeys = "vehCache.qy.lockStatus.notice";
    private Map<String, Map<String, Object>> vidLockStatus;
    private Recorder recorder = new RedisRecorder(redis);

    /**
     *
     * @param dat
     * @param vidLockStatus
     * @return 如果状态发生了变化，则发出状态变化通知报文
     */
    public Map<String, Object> carLockStatueChangeJudge(Map<String, String> dat,Map<String, Map<String, Object>> vidLockStatus) {
        if (MapUtils.isEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            Date date = new Date();
            String noticetime = timeformat.toDateString(date);
            String msgType = dat.get(SysDefine.MESSAGETYPE);
            if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(msgType)) {
                return null;
            }
            Map<String, Object> notice = null;
            if (CommandType.SUBMIT_REALTIME.equals(msgType)) {
                //当前车辆锁止状态
                String lockFunctionStatusNow = lockFunctionJudge(dat);
                String carLockStatusNow = carLockStatusJudge(dat);
                if(StringUtils.isEmpty(lockFunctionStatusNow) && StringUtils.isEmpty(carLockStatusNow)){
                    return null;
                }

                //之前的车辆锁止状态
                String lockFunctionStatusBefore = null;
                String carLockStatusBefore = null;

                if (!vidLockStatus.containsKey(vid)) {
                    setLockStatus(dat,vidLockStatus);
                    return null;
                } else {
                    Map<String, Object> lastLockStatus = vidLockStatus.get(vid);
                    lockFunctionStatusBefore = String.valueOf(lastLockStatus.get(DataKey._4710061_LOCK_FUNCTION_STATUS));
                    carLockStatusBefore = String.valueOf(lastLockStatus.get(DataKey._4710062_CAR_LOCK_STATUS));
                }

                //如果发生了变化则发出通知报文。
                if (!lockFunctionStatusNow.equals(lockFunctionStatusBefore) || !carLockStatusNow.equals(carLockStatusBefore)) {
                    notice = new TreeMap<String, Object>();
                    notice.put("msgType", "VEH_LOCK_STATUS_CHANGE");
                    notice.put("vid", vid);
                    notice.put("noticetime", noticetime);
                    notice.put("lockFunctionStatusChange", lockFunctionStatusNow);
                    notice.put("LockStatusChange", carLockStatusNow);
                    //把当前的锁止状态缓存起来
                    setLockStatus(dat,vidLockStatus);
                }
                //把车辆的锁止状态存储到redis中
                recorder.save(db, lockStatusRedisKeys,vid,vidLockStatus.get(vid));
                return notice;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     *  传过来的数据应该是实时数据报文,判断锁止功能是否开启
     * @param dat
     * @return String类型的0或1。      0：功能关闭，1：功能开启
     */
    private String lockFunctionJudge(Map<String, String> dat){
        if (MapUtils.isEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            String msgType = dat.get(SysDefine.MESSAGETYPE);
            if (StringUtils.isBlank(vid)
                    || StringUtils.isEmpty(time)
                    || StringUtils.isEmpty(msgType)) {
                return null;
            }
            //0为锁止功能关闭，1为锁止功能开启
            if (CommandType.SUBMIT_REALTIME.equals(msgType)) {
                String lockFunction = dat.get(DataKey._4710061_LOCK_FUNCTION_STATUS);
                return lockFunction;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *  传过来的数据应该是实时数据报文,判断车辆动力锁止状态
     *  需要注意的是：当锁止功能关闭时，动力锁止状态置为“禁止锁止动力”
     * @param dat
     * @return String类型的0或1或2。      0：动力未锁止，1：动力锁止， 2：禁止动力锁止
     */
    private String carLockStatusJudge(Map<String, String> dat){
        if (MapUtils.isEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            String msgType = dat.get(SysDefine.MESSAGETYPE);
            if (StringUtils.isBlank(vid)
                    || StringUtils.isEmpty(time)
                    || StringUtils.isEmpty(msgType)) {
                return null;
            }

            if (CommandType.SUBMIT_REALTIME.equals(msgType)) {
                String lockFunctionStatus = lockFunctionJudge(dat);
                //如果锁车功能状态关闭，则动力锁止状态置为“2”，即禁止锁止动力
                if("0".equals(lockFunctionStatus)){
                    return "2";
                }else if("1".equals(lockFunctionStatus)){
                    //如果锁车功能状态开启，则返回当前锁车状态，0：动力未锁止，1：动力锁止
                    String carLockStatus = dat.get(DataKey._4710062_CAR_LOCK_STATUS);
                    return carLockStatus;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void setLockStatus(Map<String, String> dat,Map<String, Map<String, Object>> vidLockStatus) {

        String lockFunctionStatus = lockFunctionJudge(dat);
        String carLockStatus = carLockStatusJudge(dat);

        if (!(StringUtils.isBlank(lockFunctionStatus) && StringUtils.isBlank(carLockStatus))) {
            Map<String, Object> lastLockStatus = new TreeMap<String, Object>();
            String vid = dat.get(DataKey.VEHICLE_ID);
            lastLockStatus.put(DataKey.VEHICLE_ID, vid);
            lastLockStatus.put(DataKey._4710061_LOCK_FUNCTION_STATUS, lockFunctionStatus);
            lastLockStatus.put(DataKey._4710062_CAR_LOCK_STATUS, carLockStatus);
            vidLockStatus.put(vid, lastLockStatus);
        }
    }
}

