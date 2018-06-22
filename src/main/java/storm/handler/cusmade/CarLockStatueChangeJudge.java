package storm.handler.cusmade;

import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.NumberUtils;
import storm.util.ObjectUtils;

import java.util.Map;
import java.util.TreeMap;

public class CarStatueChangeJudge {

    private CarOnOffHandler carOnOffhandler;


    public static Map<String, Object> carStatueChangeJudge(Map<String, String> dat){
        if (ObjectUtils.isNullOrEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            String msgType = dat.get(SysDefine.MESSAGETYPE);
            if (ObjectUtils.isNullOrEmpty(vid)
                    || ObjectUtils.isNullOrEmpty(time)
                    || ObjectUtils.isNullOrEmpty(msgType)) {
                return null;
            }
            Map<String, Object> notice = null;
            if (CommandType.SUBMIT_REALTIME.equals(msgType)) {

                String mileage = dat.get(DataKey._2202_TOTAL_MILEAGE);//当前总里程

                if (vidLastDat.containsKey(vid)) {
                    //mileage如果是数字字符串则返回，不是则返回字符串“0”
                    mileage = NumberUtils.stringNumber(mileage);
                    if (!"0".equals(mileage)) {
                        int mile = Integer.parseInt(mileage);//当前总里程
                        Map<String, Object> lastMap = vidLastDat.get(vid);
                        int lastMile = (int) lastMap.get(DataKey._2202_TOTAL_MILEAGE);//上一帧的总里程
                        int nowmileHop = Math.abs(mile - lastMile);//里程跳变

                        if (nowmileHop >= mileHop) {
                            String lastTime = (String) lastMap.get(DataKey.TIME);//上一帧的时间即为跳变的开始时间
                            String vin = dat.get(DataKey.VEHICLE_NUMBER);
                            notice = new TreeMap<String, Object>();
                            notice.put("msgType", "HOP_MILE");//这些字段是前端方面要求的。
                            notice.put("vid", vid);
                            notice.put("vin", vin);
                            notice.put("stime", lastTime);
                            notice.put("etime", time);
                            notice.put("stmile", lastMile);
                            notice.put("edmile", mile);
                            notice.put("hopValue", nowmileHop);
                        }
                    }
                }
                //如果vidLastDat中没有缓存此vid，则把这一帧报文中的vid、time、mileage字段缓存到LastDat
                setLastDat(dat);
            }

            return notice;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //传过来的数据应该是实时数据报文，返回车辆当前状态
    public String realStatus(Map<String, String> dat){
        if (ObjectUtils.isNullOrEmpty(dat)) {
            return null;
        }

        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            String msgType = dat.get(SysDefine.MESSAGETYPE);
            String chargeStatus = dat.get(DataKey._2301_CHARGE_STATUS);
            if (ObjectUtils.isNullOrEmpty(vid)
                    || ObjectUtils.isNullOrEmpty(time)
                    || ObjectUtils.isNullOrEmpty(msgType)) {
                return null;
            }
            //如果为终端锁车报文，再去做判断
            if (CommandType.SUBMIT_TERMLOCK.equals(msgType)) {
                carOnOffhandler = new CarOnOffHandler();
                boolean judgeOffine = carOnOffhandler.isOffline(dat);
                if(!judgeOffine){
                    return "onLine";
                }else{
                    if("1" == chargeStatus){
                        return "charging";
                    }else{
                        return "offLine";
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    //传过来的数据应该是实时数据报文,返回锁止功能是否开启
    public String lockIsOpen(Map<String, String> dat){
        if (ObjectUtils.isNullOrEmpty(dat)) {
            return null;
        }

        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            String msgType = dat.get(SysDefine.MESSAGETYPE);
            String chargeStatus = dat.get(DataKey._2301_CHARGE_STATUS);
            if (ObjectUtils.isNullOrEmpty(vid)
                    || ObjectUtils.isNullOrEmpty(time)
                    || ObjectUtils.isNullOrEmpty(msgType)) {
                return null;
            }
            //如果为终端锁车报文，再去做判断
            if (CommandType.SUBMIT_TERMLOCK.equals(msgType)) {
                carOnOffhandler = new CarOnOffHandler();
                boolean judgeOffine = carOnOffhandler.isOffline(dat);
                if(!judgeOffine){
                    return "onLine";
                }else{
                    if("1" == chargeStatus){
                        return "charging";
                    }else{
                        return "offLine";
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }




}

