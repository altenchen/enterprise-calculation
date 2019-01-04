package storm.handler.cusmade;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import storm.constant.FormatConstant;
import storm.system.DataKey;
import storm.system.NoticeType;

import java.util.*;

/**
 * 车辆点火熄火通知
 *
 * @author 智杰
 */
public class CarIgniteShutJudge {

    /**
     * 车辆点火统计
     * <vid, 计数>
     */
    private Map<String, Integer> vidIgnite = new HashMap<>();
    /**
     * 车辆熄火统计
     * <vid, 计数>
     */
    private Map<String, Integer> vidShut = new HashMap<>();
    /**
     * 车辆点火至熄火这段期间最大车速
     * <vid, speed>
     */
    private Map<String, Double> igniteShutMaxSpeed = new HashMap<>();
    /**
     * 最好一帧soc
     * <vid, soc>
     */
    private Map<String, Double> lastSoc = new HashMap<>();
    /**
     * 最后一帧里程
     * <vid, mile>
     */
    private Map<String, Double> lastMile = new HashMap<>();
    /**
     * 车辆点火熄火通知
     * <vid, notice>
     */
    private Map<String, Map<String, Object>> vidIgniteShutNotice = new HashMap<>();

    /**
     * IGNITE_SHUT_MESSAGE
     * 点火熄火
     *
     * @param dat
     * @return
     */
    public Map<String, Object> processFrame(Map<String, String> dat) {
        if (MapUtils.isEmpty(dat)) {
            return null;
        }
        try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String vin = dat.get(DataKey.VEHICLE_NUMBER);
            String time = dat.get(DataKey.TIME);
            String carStatus = dat.get(DataKey._3201_CAR_STATUS);
            if (StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)
                || StringUtils.isEmpty(carStatus)) {
                return null;
            }

            String latit = dat.get(DataKey._2503_LATITUDE);
            String longi = dat.get(DataKey._2502_LONGITUDE);
            String location = longi + "," + latit;
            String noticetime = DateFormatUtils.format(new Date(), FormatConstant.DATE_FORMAT);
            String speed = dat.get(DataKey._2201_SPEED);
            String socStr = dat.get(DataKey._7615_STATE_OF_CHARGE);
            String mileageStr = dat.get(DataKey._2202_TOTAL_MILEAGE);

            double soc = -1;
            if (!StringUtils.isEmpty(socStr)) {
                soc = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(socStr) ? socStr : "0");
                if (-1 != soc) {

                    lastSoc.put(vid, soc);
                }
            } else {
                if (lastSoc.containsKey(vid)) {
                    soc = lastSoc.get(vid);
                }
            }
            double mileage = -1;
            if (!StringUtils.isEmpty(mileageStr)) {
                mileage = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(mileageStr) ? mileageStr : "0");
                if (-1 != mileage) {

                    lastMile.put(vid, mileage);
                }
            } else {
                if (lastMile.containsKey(vid)) {
                    mileage = lastMile.get(vid);
                }
            }

            double maxSpd = -1;
            if ("1".equals(carStatus)
                || "2".equals(carStatus)) {
                double spd = -1;
                if (!igniteShutMaxSpeed.containsKey(vid)) {
                    igniteShutMaxSpeed.put(vid, maxSpd);
                }
                if (!StringUtils.isEmpty(speed)) {

                    maxSpd = igniteShutMaxSpeed.get(vid);
                    spd = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(speed) ? speed : "0");
                    if (spd > maxSpd) {
                        maxSpd = spd;
                        igniteShutMaxSpeed.put(vid, maxSpd);
                    }
                }

            }
            if ("1".equals(carStatus)) {
                //点火
                int cnts = 0;
                if (vidIgnite.containsKey(vid)) {
                    cnts = vidIgnite.get(vid);
                }
                cnts++;
                vidIgnite.put(vid, cnts);
                if (vidShut.containsKey(vid)) {
                    vidShut.remove(vid);
                }
                if (cnts >= 2) {

                    Map<String, Object> notice = vidIgniteShutNotice.get(vid);
                    if (null == notice) {
                        //生成点火通知
                        notice = new TreeMap<>();
                        notice.put("msgType", NoticeType.IGNITE_SHUT_MESSAGE);
                        notice.put("vid", vid);
                        notice.put("vin", vin);
                        notice.put("msgId", UUID.randomUUID().toString());
                        notice.put("stime", time);
                        notice.put("soc", soc);
                        notice.put("ssoc", soc);
                        notice.put("mileage", mileage);
                        notice.put("status", 1);
                        notice.put("location", location);
                    } else {
                        double ssoc = (double) notice.get("ssoc");
                        double energy = Math.abs(ssoc - soc);
                        notice.put("soc", soc);
                        notice.put("mileage", mileage);
                        notice.put("maxSpeed", maxSpd);
                        notice.put("energy", energy);
                        notice.put("status", 2);
                        notice.put("location", location);
                    }
                    notice.put("noticetime", noticetime);
                    vidIgniteShutNotice.put(vid, notice);

                    if (1 == (int) notice.get("status")) {
                        return notice;
                    }
                }
            } else if ("2".equals(carStatus)) {
                //熄火
                if (vidIgnite.containsKey(vid)) {
                    int cnts = 0;
                    if (vidShut.containsKey(vid)) {
                        cnts = vidShut.get(vid);
                    }
                    cnts++;
                    vidShut.put(vid, cnts);

                    if (cnts >= 1) {
                        Map<String, Object> notice = vidIgniteShutNotice.get(vid);
                        vidIgnite.remove(vid);
                        vidIgniteShutNotice.remove(vid);

                        if (null != notice) {
                            double ssoc = (double) notice.get("ssoc");
                            double energy = Math.abs(ssoc - soc);
                            notice.put("soc", soc);
                            notice.put("mileage", mileage);
                            notice.put("maxSpeed", maxSpd);
                            notice.put("energy", energy);
                            notice.put("status", 3);
                            notice.put("location", location);
                            notice.put("etime", time);
                            notice.put("noticetime", noticetime);
                            vidShut.remove(vid);
                            return notice;
                        }
                    }
                }
                igniteShutMaxSpeed.remove(vid);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
