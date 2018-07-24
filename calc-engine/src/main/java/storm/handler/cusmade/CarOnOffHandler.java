package storm.handler.cusmade;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.NotNull;
import storm.cache.SysRealDataCache;
import storm.constant.FormatConstant;
import storm.handler.ctx.Recorder;
import storm.handler.ctx.RedisRecorder;
import storm.protocol.CommandType;
import storm.protocol.SUBMIT_LINKSTATUS;
import storm.protocol.SUBMIT_LOGIN;
import storm.system.DataKey;
import storm.system.ProtocolItem;
import storm.system.SysDefine;

/**
 * 车辆上下线及相关处理
 */
public final class CarOnOffHandler implements OnOffInfoNotice {

    private final Map<String, Map<String, Object>> vidIdleNotice = new HashMap<>();
    private final Map<String, Map<String, Object>> onOffMileNotice = new HashMap<>();
    private final Map<String, TimeMileage> vidLastTimeMile = new HashMap<>();
    private final Map<String, Integer> vidLastSpeed = new HashMap<>();
    private final Map<String, Integer> vidLastMileage = new HashMap<>();
    private final Map<String, Integer> vidLastSoc = new HashMap<>();
    private final Recorder recorder = new RedisRecorder();
    private static final int REDIS_DB_INDEX =6;
    private static final String IDLE_REDIS_KEYS = "vehCache.qy.idle";

    {
        //重要：从redis数据库中读取系统重启前的车辆状态。不写的话，当系统重启时，会导致车辆的状态丢失
        //例如，就算车辆上线了，因为收不到闲置结束通知，闲置车辆可能也会一直在数据库中。
        restartInit(true);
    }

    @Override
    public Map<String, Object> generateNotices(@NotNull Map<String, String> dat, long now, long timeout) {
        Map<String, Object> notice = onoffMile(dat, now, timeout);
        return notice;
    }

    /**
     * 扫描所有车辆数据，找出闲置车辆，车辆闲置通知，并返回从缓存中清除。
     * 注意：此处定义的所有车辆可以理解为只有两种状态，（闲置和活跃），下线了也还属于活跃，只有下线时间超过阈值才归为闲置
     * 闲置车辆将从活跃缓存车辆中删除
     * @param type
     * @param status
     * @param now
     * @param timeout
     * @return 闲置车辆通知
     */
    @Override
    public List<Map<String, Object>> fulldoseNotice(String type, ScanRange status, long now, long timeout) {//status:0全量数据，status:1活跃数据，status:2其他定义
        if ("TIMEOUT".equals(type)) {
            Map<String,Map<String,String>> cluster = null;
            //使用这个队列是为了防止在访问vids时，发生修改，引发错误。
            LinkedBlockingQueue<String> vids = null;
            //1、先从队列中把数据拿出来，进行是否为闲置的判断。缓存的是整条车辆报文
            if (ScanRange.AllData == status) {
                cluster=SysRealDataCache.getDataCache().asMap();
                vids = SysRealDataCache.lasts;
            } else if (ScanRange.AliveData == status) {
                cluster=SysRealDataCache.getLivelyCache().asMap();
                vids = SysRealDataCache.alives;
            }
            if (null != cluster && cluster.size()>0
                    && null !=vids && vids.size() >0) {
                List<Map<String, Object>> list = new LinkedList<Map<String, Object>>();
                List<String> markDel =  new LinkedList<String>();
                List<String> markAlives =  new LinkedList<String>();
                List<String> allCars =  new LinkedList<String>();

                //循环访问队列中的vid，并清空队列
                String vid = vids.poll();
                while(null != vid){
                    if (ScanRange.AllData == status){
                        SysRealDataCache.removeLastQueue(vid);
                        allCars.add(vid);
                    }else if (ScanRange.AliveData == status) {
                        SysRealDataCache.removeAliveQueue(vid);
                    }
                    Map<String,String> dat = cluster.get(vid);
                    //闲置车辆通知
                    Map<String, Object> notice = inidle(dat, now, timeout,markDel,markAlives);
                    if (null != notice) {
                        list.add(notice);
                    }
                    vid = vids.poll();
                }

                //2、根据上面的判断，把闲置的车辆从活跃车辆列表中剔除，活跃车辆再次放入队列

                /**
                 * remove cache
                 */
                if (markDel.size() > 0) {
                    for (String key : markDel) {
                        cluster.remove(key);
                        SysRealDataCache.removeAliveQueue(key);
                    }
                }

                /**
                 * 活跃车辆再次加入队列
                 */
                if (markAlives.size() > 0) {
                    for (String key : markAlives) {

                        SysRealDataCache.addAliveQueue(key);
                    }
                }

                //3、在把所有的数据放回实时数据缓存，当然也包括已经闲置的车辆。

                /**
                 * 最后一帧车辆再次加入队列
                 */
                if (ScanRange.AllData == status && allCars.size() > 0) {

                    for (String key : allCars) {

                        SysRealDataCache.addLastQueue(key);
                    }
                }

                /**
                 * return result
                 */
                if (list.size() > 0) {
                    return list;
                }
            }
        }
        return null;
    }

    /**
     * 此方法检查离线。
     * 里面逻辑有问题，可以优化
     * @param type
     * @param status
     * @param now
     * @param timeout
     * @return
     */
    @Override
    public void onOffCheck(String type, int status, long now, long timeout) {
        if ("TIMEOUT".equals(type)) {
            Map<String,Map<String,String>> cluster = null;
            //LinkedBlockingQueue是一个单向链表实现的阻塞队列，先进先出的顺序。支持多线程并发操作。无界队列。
            LinkedBlockingQueue<String> vids = null;
            if (0 == status) {
                //获取集群中车辆最后一条数据
                cluster=SysRealDataCache.getDataCache().asMap();
                vids = SysRealDataCache.lasts;
            } else if (1 == status) {
                cluster=SysRealDataCache.getLivelyCache().asMap();
                vids = SysRealDataCache.alives;
            }
            if (null != cluster && cluster.size()>0
                    && null !=vids && vids.size() >0) {

                List<String> allCars =  new LinkedList<String>();
                List<String> markAlives =  new LinkedList<String>();
                //poll是队列数据结构实现类的方法，从队首获取元素，同时获取的这个元素将从原队列删除；
                String vid = vids.poll();
                //循环访问队列中的vid，并清空队列
                while(null != vid){

                    if (0 == status){
                        SysRealDataCache.removeLastQueue(vid);
                    }else if (1 == status) {
                        SysRealDataCache.removeAliveQueue(vid);
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

                        SysRealDataCache.addAliveQueue(key);
                    }
                }
                /**
                 * 最后一帧车辆再次加入队列
                 */
                if (0 == status && allCars.size() > 0) {

                    for (String key : allCars) {

                        SysRealDataCache.addLastQueue(key);
                    }
                }

            }
        }
    }

    /**
     * 判断是否为闲置或者停机车辆。（重要）
     *
     * 车辆在系统中的最后一帧有效数据
     * @param dat
     * 系统当前时间
     * @param now
     * 超时时间，（通过判断系统当前时间与最后一帧有效数据的时间差是否大于超时时间）
     * @param timeout
     * 需要从活跃车辆列表中删除的活跃车辆
     * @param markDel
     * 状态变为活跃的车辆
     * @param markAlive
     *
     * @return 闲置开始通知或者闲置结束通知，或者null
     *
     */
    private Map<String, Object> inidle(Map<String, String> dat,long now,long timeout,List<String> markDel,List<String> markAlive){
        if (null == dat || dat.size() ==0) {
            return null;
        }

        String vid = dat.get(DataKey.VEHICLE_ID);
        String time = dat.get(DataKey.TIME);
        String msgType = dat.get(SysDefine.MESSAGETYPE);
        if (StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)) {
            return null;
        }
        //速度和soc为预留字段，当前没有用到
        int numSpeed = -1;
        int numSoc = -1;
        int numMileage = -1;
        try {
            if (CommandType.SUBMIT_REALTIME.equals(msgType)){

                String speed = dat.get(DataKey._2201_SPEED);
                String soc = dat.get(DataKey._7615_STATE_OF_CHARGE);
                String mileage = dat.get(DataKey._2202_TOTAL_MILEAGE);
                //下面三个if类似，都是校验一下，增强健壮性然后将vid和最后一帧的数据存入
                if (null !=speed && !"".equals(speed)) {
                    speed = org.apache.commons.lang.math.NumberUtils.isNumber(speed) ? speed : "0";
                    int posidx = speed.indexOf(".");
                    if (posidx != -1) {
                        speed = speed.substring(0, posidx);
                        speed = "".equals(speed)?"0":speed;
                    }
                    if (! "0".equals(speed)) {
                        numSpeed = Integer.parseInt(speed);
                        vidLastSpeed.put(vid, numSpeed);
                    }
                }

                if (null !=soc && !"".equals(soc)) {
                    soc = org.apache.commons.lang.math.NumberUtils.isNumber(soc) ? soc : "0";
                    int posidx = soc.indexOf(".");
                    if (posidx != -1) {
                        soc = soc.substring(0, posidx);
                        soc = "".equals(soc)?"0":soc;
                    }
                    if (! "0".equals(soc)) {
                        numSoc = Integer.parseInt(soc);
                        vidLastSoc.put(vid, numSoc);
                    }
                }
                if (null !=mileage && !"".equals(mileage)) {
                    mileage = org.apache.commons.lang.math.NumberUtils.isNumber(mileage) ? mileage : "0";
                    int posidx = mileage.indexOf(".");
                    if (posidx != -1) {
                        mileage = mileage.substring(0, posidx);
                        mileage = "".equals(mileage)?"0":mileage;
                    }
                    //当报文中的mileage值无效时，经过上面的处理，此处mileage为0
                    if (! "0".equals(mileage)) {
                        //mileage有效
                        numMileage = Integer.parseInt(mileage);
                        vidLastMileage.put(vid, numMileage);
                    }else{
                        //mileage无效，无效则取最后一帧有效的值，如果没有缓存最后一帧有效值，则置为-1，由前端处理展示
                        if(null != vidLastMileage.get(vid)){
                            numMileage = vidLastMileage.get(vid);
                            vidLastMileage.put(vid, numMileage);
                        }
                    }
                }
            }else{
                if(null != vidLastSpeed.get(vid)){
                    numSpeed = vidLastSpeed.get(vid);
                }

                if(null != vidLastSoc.get(vid)){
                    numSoc = vidLastSoc.get(vid);
                }

                if(null != vidLastMileage.get(vid)){
                    numMileage = vidLastMileage.get(vid);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        String lastUtc = dat.get(SysDefine.ONLINEUTC);
        String noticetime = DateFormatUtils.format(new Date(now), FormatConstant.DATE_FORMAT);

        //是否为登入报文
        boolean isLogin = CommandType.SUBMIT_LOGIN.equals(dat.get(SysDefine.MESSAGETYPE));
        //车辆 是否达到 闲置或者停机 超时的标准
        //判断标准就是当前时间与缓存中的最后一帧报文时间差值是否大于阈值，
        //需要注意的是，此时已经的下线车辆也是在全量数据或者活跃数据缓存中的。
        boolean isout = istimeout(time, lastUtc, now, timeout);
        if (isout) {//是闲置车辆
            Map<String, Object> notice = vidIdleNotice.get(vid);
            if (null == notice) {
                notice =  new TreeMap<String, Object>();
                notice.put("msgType", "IDLE_VEH");
                notice.put("vid", vid);
                notice.put("msgId", UUID.randomUUID().toString());
                notice.put("stime", time);
                notice.put("soc", numSoc);
                //吉利要求，新增
                notice.put("smileage", numMileage);
                notice.put("speed", numSpeed);
                notice.put("status", 1);
                //吉利要求，新增
                notice.put("offlineMillisecondsThreshold", timeout);
            }else{
                final Object smileage = notice.get("smileage");
                if(smileage == null || "-1".equals(smileage.toString())) {
                    notice.put("smileage", numMileage);
                }
                notice.put("status", 2);
            }
            notice.put("noticetime", noticetime);
            vidIdleNotice.put(vid, notice);
            /**
             * 添加删除标记从 cache 移除
             */
            markDel.add(vid);
            if (1 == (int)notice.get("status")) {
                recorder.save(REDIS_DB_INDEX, IDLE_REDIS_KEYS,vid, notice);
                return notice;
            }
        } else {//不是闲置车辆
            markAlive.add(vid);
            //如果是登入报文，则返回null。针对吉利自动唤醒报文
            if(isLogin){
                return null;
            }

            if (vidIdleNotice.containsKey(vid)) {
                int lastSoc = -1;
                int lastSpeed = -1;
                int lastMileage = -1;
                if (vidLastSoc.containsKey(vid)) {
                    lastSoc = vidLastSoc.get(vid);
                }
                if (vidLastSpeed.containsKey(vid)) {
                    lastSpeed = vidLastSpeed.get(vid);
                }
                if (vidLastMileage.containsKey(vid)) {
                    lastMileage = vidLastMileage.get(vid);
                }
                Map<String, Object> notice = vidIdleNotice.get(vid);
                vidIdleNotice.remove(vid);
                //删除redis中的闲置车辆数据
                recorder.del(REDIS_DB_INDEX, IDLE_REDIS_KEYS, vid);
                //发送结束报文
                if (null != notice) {
                    notice.put("status", 3);
                    notice.put("etime", time);
                    notice.put("noticetime", noticetime);
                    notice.put("soc", lastSoc);
                    notice.put("mileage", lastMileage);
                    //吉利要求，新增
                    notice.put("emileage", lastMileage);
                    notice.put("speed", lastSpeed);
                    return notice;
                }
            }
        }
        return null;
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
        String msgType = dat.get(SysDefine.MESSAGETYPE);
        String vid = dat.get(DataKey.VEHICLE_ID);
        String time = dat.get(DataKey.TIME);
        if (StringUtils.isEmpty(msgType)
                || StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)) {
            return null;
        }
        String lastUtc = dat.get(SysDefine.ONLINEUTC);
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
        boolean isout = istimeout(time, lastUtc, now, timeout);
        if (isoff || isout) {
            TimeMileage timeMileage = vidLastTimeMile.get(vid);
            if (null != timeMileage
                    && timeMileage.mileage>0
                    && !onOffMileNotice.containsKey(vid)) {
                Map<String, Object> notice =  new TreeMap<String, Object>();
                notice.put("msgType", "ON_OFF_MILE");
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
     * @param dat
     * @param now
     * @param timeout
     * @return
     */
    private Map<String, Object> onoffMile(Map<String, String> dat,long now,long timeout){
        if (null == dat || dat.size() ==0) {
            return null;
        }
        String msgType = dat.get(SysDefine.MESSAGETYPE);
        String vid = dat.get(DataKey.VEHICLE_ID);
        String time = dat.get(DataKey.TIME);
        if (StringUtils.isEmpty(msgType)
                || StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)) {
            return null;
        }
        String lastUtc = dat.get(SysDefine.ONLINEUTC);
        String noticetime = DateFormatUtils.format(new Date(now), FormatConstant.DATE_FORMAT);
        double lastmileage = -1;
        if (dat.containsKey(DataKey._2202_TOTAL_MILEAGE)) {
            String str = dat.get(DataKey._2202_TOTAL_MILEAGE);
            String mileage = org.apache.commons.lang.math.NumberUtils.isNumber(str) ? str : "0";
            if (! "0".equals(mileage)) {

                lastmileage = Double.parseDouble(mileage);
                if (-1 != lastmileage) {
                    vidLastTimeMile.put(vid, new TimeMileage(now,time,lastmileage));
                }
            }
        }
        boolean isoff = isOffline(dat);
        boolean isout = istimeout(time, lastUtc, now, timeout);
        //根据报文判断离线了，或者，很长时间没发报文了，判断为离线
        if (isoff || isout) {
            TimeMileage timeMileage = vidLastTimeMile.get(vid);
            if (null != timeMileage
                    && timeMileage.mileage>0
                    && !onOffMileNotice.containsKey(vid)) {
                Map<String, Object> notice =  new TreeMap<String, Object>();
                notice.put("msgType", "ON_OFF_MILE");
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
    private boolean istimeout(String time, String lastUtc,long now,long timeout){

        if (null == time && null == lastUtc) {
            return false;
        }
        try {
            long tmp_last = Long.parseLong(org.apache.commons.lang.math.NumberUtils.isNumber(lastUtc) ? lastUtc : "0");
            long last = DateUtils.parseDate(String.valueOf(tmp_last), new String[]{FormatConstant.DATE_FORMAT}).getTime();
            long tertime = DateUtils.parseDate(time, new String[]{FormatConstant.DATE_FORMAT}).getTime();
            long maxtime = Math.max(last, tertime);
            if (now - maxtime > timeout) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     * 判断车辆是否离线，最本质的判断离线方法，根据报文不同类型采用不同方法判断
     * @param dat
     * @return 是否离线
     */

    private boolean isOffline(Map<String, String> dat){
        String msgType = dat.get(SysDefine.MESSAGETYPE);
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

    void restartInit(boolean isRestart){
            if (isRestart) {
            recorder.rebootInit(REDIS_DB_INDEX, IDLE_REDIS_KEYS, vidIdleNotice);
        }
    }
}
