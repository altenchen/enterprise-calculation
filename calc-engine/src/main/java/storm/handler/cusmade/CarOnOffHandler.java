package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.SysRealDataCache;
import storm.cache.VehicleCache;
import storm.constant.FormatConstant;
import storm.protocol.CommandType;
import storm.protocol.SUBMIT_LINKSTATUS;
import storm.protocol.SUBMIT_LOGIN;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.system.ProtocolItem;
import storm.system.SysDefine;

import java.io.Serializable;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 1. 闲置车辆通知(已转移待清理)
 * 2. 车辆上线里程通知(待转移)
 */
public final class CarOnOffHandler implements Serializable {

    private static final long serialVersionUID = 3816584437913891275L;

    private static final Logger LOG = LoggerFactory.getLogger(CarNoCanJudge.class);

    private final Map<String, Map<String, Object>> onOffMileNotice = new HashMap<>();
    private final Map<String, TimeMileage> vidLastTimeMile = new HashMap<>();
    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    /**
     * TODO: 独立出去
     * 生成通知
     * @param data
     * @param now
     * @param timeout
     * @return
     */
    public Map<String, Object> generateNotices(@NotNull final ImmutableMap<String, String> data, long now, long timeout) {
        Map<String, Object> notice = onoffMile(data, now, timeout);
        return notice;
    }

    /**
     * 此方法检查离线。
     *
     * @param type
     * @param status
     * @param now
     * @param timeout
     * @return
     */
    public void onOffCheck(String type, int status, long now, long timeout) {
        if ("TIMEOUT".equals(type)) {
            Map<String,Map<String,String>> cluster = null;
            //LinkedBlockingQueue是一个单向链表实现的阻塞队列，先进先出的顺序。支持多线程并发操作。无界队列。
            LinkedBlockingQueue<String> vids = null;
            if (0 == status) {
                //获取集群中车辆最后一条数据
                cluster=SysRealDataCache.getLastDataCache();
                vids = SysRealDataCache.LAST_DATA_QUEUE;
            } else if (1 == status) {
                cluster=SysRealDataCache.getAliveCarCache();
                vids = SysRealDataCache.ALIVE_CAR_QUEUE;
            }
            if (null != cluster && cluster.size()>0
                    && null !=vids && vids.size() >0) {

                List<String> allCars = new LinkedList<>();
                List<String> markAlives = new LinkedList<>();
                //poll是队列数据结构实现类的方法，从队首获取元素，同时获取的这个元素将从原队列删除；
                String vid = vids.poll();
                //循环访问队列中的vid，并清空队列
                while(null != vid){

                    if (0 == status){
                        SysRealDataCache.removeFromLastDataSet(vid);
                    }else if (1 == status) {
                        SysRealDataCache.removeFromAliveSet(vid);
                    }
                    allCars.add(vid);

                    Map<String,String> dat = cluster.get(vid);
                    offMile(dat, now, timeout,markAlives);
                    vid = vids.poll();
                }

                /**
                 * 活跃车辆再次加入队列
                 */
                if (markAlives.size() > 0) {
                    for (String key : markAlives) {

                        SysRealDataCache.addToAliveCarQueue(key);
                    }
                }
                /**
                 * 最后一帧车辆再次加入队列
                 */
                if (0 == status && allCars.size() > 0) {

                    for (String key : allCars) {

                        SysRealDataCache.addToLastDataQueue(key);
                    }
                }

            }
        }
    }

    /**
     *判断车辆是否下线，如果下线了，则将车辆下线通知放到onOffMileNotice缓存。
     * 同时将所有的车辆id放到markAlive中。
     * @param dat
     * @param now
     * @param timeout
     * @param markAlive
     * @return
     */
    private Map<String, Object> offMile(Map<String, String> dat,long now,long timeout,List<String> markAlive){
        if (null == dat || dat.size() ==0) {
            return null;
        }
        String msgType = dat.get(DataKey.MESSAGE_TYPE);
        String vid = dat.get(DataKey.VEHICLE_ID);
        String time = dat.get(DataKey.TIME);
        if (StringUtils.isEmpty(msgType)
                || StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)) {
            return null;
        }
        String lastUtc = dat.get(SysDefine.ONLINE_UTC);
        double lastmileage = -1;
        if (dat.containsKey(DataKey._2202_TOTAL_MILEAGE)) {
            String str = dat.get(DataKey._2202_TOTAL_MILEAGE);
            lastmileage = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(str) ? str : "0");
            if (-1 != lastmileage) {
                vidLastTimeMile.put(vid, new TimeMileage(now,time,lastmileage));
            }
        }
        //车辆是否离线
        boolean isoff = isOffline(dat);
        //车辆 是否达到 闲置或者停机 超时的标准
        boolean isout = isTimeout(time, lastUtc, now, timeout);
        if (isoff || isout) {
            TimeMileage timeMileage = vidLastTimeMile.get(vid);
            if (null != timeMileage
                    && timeMileage.mileage>0
                    && !onOffMileNotice.containsKey(vid)) {
                Map<String, Object> notice = new TreeMap<>();
                notice.put("msgType", NoticeType.ON_OFF_MILE);
                notice.put("vid", vid);
                notice.put("stime", time);
                notice.put("smileage", timeMileage.mileage);
                onOffMileNotice.put(vid, notice);
            }

        }
        //原本是有else的，只有不是下线车辆的时候才放到markAlive缓存中。
        //但是为了判断闲置车辆，当车辆下线了也要先放到markAlive缓存中。
        markAlive.add(vid);
        return null;
    }
    /**
     *
     * @param immutableMap
     * @param now
     * @param timeout
     * @return
     */
    private Map<String, Object> onoffMile(@NotNull final ImmutableMap<String, String> immutableMap,long now,long timeout){
        if (null == immutableMap || immutableMap.size() ==0) {
            return null;
        }

        final Map<String, String> data = Maps.newHashMap(immutableMap);

        String msgType = data.get(DataKey.MESSAGE_TYPE);
        String vid = data.get(DataKey.VEHICLE_ID);
        String time = data.get(DataKey.TIME);
        if (StringUtils.isEmpty(msgType)
                || StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)) {
            return null;
        }
        String lastUtc = data.get(SysDefine.ONLINE_UTC);
        String noticetime = DateFormatUtils.format(new Date(now), FormatConstant.DATE_FORMAT);
        double lastmileage = -1;
        if (data.containsKey(DataKey._2202_TOTAL_MILEAGE)) {
            String str = data.get(DataKey._2202_TOTAL_MILEAGE);
            String mileage = org.apache.commons.lang.math.NumberUtils.isNumber(str) ? str : "0";
            if (! "0".equals(mileage)) {

                lastmileage = Double.parseDouble(mileage);
                if (-1 != lastmileage) {
                    vidLastTimeMile.put(vid, new TimeMileage(now,time,lastmileage));
                }
            }
        }
        boolean isoff = isOffline(data);
        boolean isout = isTimeout(time, lastUtc, now, timeout);
        //根据报文判断离线了，或者，很长时间没发报文了，判断为离线
        if (isoff || isout) {
            TimeMileage timeMileage = vidLastTimeMile.get(vid);
            if (null != timeMileage
                    && timeMileage.mileage>0
                    && !onOffMileNotice.containsKey(vid)) {
                Map<String, Object> notice = new TreeMap<>();
                notice.put("msgType", NoticeType.ON_OFF_MILE);
                notice.put("vid", vid);
                notice.put("stime", time);
                notice.put("smileage", timeMileage.mileage);
                onOffMileNotice.put(vid, notice);
            }

        } else {
            if (CommandType.SUBMIT_REALTIME.equals(msgType)
                    && -1 != lastmileage){

                if (onOffMileNotice.containsKey(vid)) {
                    Map<String, Object> notice = onOffMileNotice.get(vid);
                    onOffMileNotice.remove(vid);
                    if (null != notice) {
                        notice.put("etime", time);
                        notice.put("emileage", lastmileage);
                        notice.put("noticetime", noticetime);
                        return notice;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 车辆 是否达到 闲置或者停机 超时的标准
     */
    private boolean isTimeout(final String time, final String lastUtc, final long currentTimeMillis, final long idleTimeout){

        long timeValue = 0;
        if(NumberUtils.isDigits(time)) {
            try {
                timeValue = DateUtils.parseDate(time, new String[]{FormatConstant.DATE_FORMAT}).getTime();
            } catch (ParseException e) {
                LOG.warn("闲置时间是否超时判断中报出异常", e);
            }
        }

        final long utcValue = NumberUtils.toLong(lastUtc, 0);

        // 车辆TIME数据时间 和 数据帧到达 storm 时间(utc) 中更大的值.
        // 这里有点问题:
        // 1. 几乎总是使用utc时间, 而这个时间做闲置条件并不合适
        // 2. TIME 在实时数据中来源于车载终端采集时间, 这个值误差很大.
        final long maxTime = Math.max(timeValue, utcValue);

        if (maxTime > 0 && currentTimeMillis - maxTime > idleTimeout) {
            return true;
        }
        return false;
    }

    /**
     * 判断车辆是否离线，最本质的判断离线方法，根据报文不同类型采用不同方法判断
     * @param dat
     * @return 是否离线
     */

    private boolean isOffline(Map<String, String> dat){
        String msgType = dat.get(DataKey.MESSAGE_TYPE);
        if (CommandType.SUBMIT_LOGIN.equals(msgType)) {
            //1、先根据自带的TYPE字段进行判断。平台注册通知类型 0:从未上过线，1:车机终端上线 ，2:车机离线，3:平台上线，4:平台下线
            String type = dat.get(ProtocolItem.REG_TYPE);
            if ("1".equals(type)){
                return false;
            } else if ("2".equals(type)){
                return true;
            } else {
                //2、如果自带的type字段没数据，则根据登入登出流水号判断。
                String logoutSeq = dat.get(SUBMIT_LOGIN.LOGOUT_SEQ);
                String loginSeq = dat.get(SUBMIT_LOGIN.LOGIN_SEQ);
                if (!StringUtils.isEmpty(logoutSeq)
                        && !StringUtils.isEmpty(logoutSeq)) {
                    int logout = Integer.parseInt(org.apache.commons.lang.math.NumberUtils.isNumber(logoutSeq) ? logoutSeq : "0");
                    int login = Integer.parseInt(org.apache.commons.lang.math.NumberUtils.isNumber(loginSeq) ? loginSeq : "0");
                    if(login >logout){
                        return false;
                    }
                    return true;

                } else{
                    if (StringUtils.isEmpty(loginSeq)) {
                        return false;
                    }
                    return true;
                }
            }
        } else if (CommandType.SUBMIT_LINKSTATUS.equals(msgType)){
            //3、如果是链接状态通知，则根据连接状态字段进行判断，1上线，2心跳，3离线
            String linkType = dat.get(SUBMIT_LINKSTATUS.LINK_TYPE);
            if ("1".equals(linkType)
                    ||"2".equals(linkType)) {
                return false;
            } else if("3".equals(linkType)){
                return true;
            }
        } else if (CommandType.SUBMIT_REALTIME.equals(msgType)){
            //4、如果是实时数据直接返回false
            return false;
        }
        return false;
    }
}
