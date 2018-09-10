package storm.handler.cal;

import java.text.ParseException;
import java.text.SimpleDateFormat;
//import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.protocol.CommandType;
import storm.protocol.SUBMIT_LINKSTATUS;
import storm.protocol.SUBMIT_LOGIN;
import storm.system.DataKey;
import storm.system.ProtocolItem;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;

public class EsRealCalHandler {

    private static Logger LOG = LoggerFactory.getLogger(EsRealCalHandler.class);

    private static final ConfigUtils CONFIG_UTILS = ConfigUtils.getInstance();

    private static ThreadLocal<SimpleDateFormat> inFormatlocal = new ThreadLocal<>();
    private static ThreadLocal<SimpleDateFormat> outFormatlocal = new ThreadLocal<>();
    private static Map<String, Map<String, String>> zeroCache = new java.util.concurrent.ConcurrentHashMap<>();
    private static long onlinetime = 180 * 1000;
    private static long stoptime = 180 * 1000;
    private static long oncesend = 60000;//每隔多少时间推送一次,默认一分钟，60000毫秒。如果负数或者0代表实时推送;
    private static boolean clusterSend = false;
    private static boolean carinfoSend = false;
    private long lastsendtime;//最后一次推送时间
    private Map<String, Long> carlasttimes;//最后一条数据时间
    private RegHanler regHanler;

    /**
     * 在线车辆数据缓存
     */
    public static final Cache<String, Map<String, String>> statusAliveCars = CacheBuilder.newBuilder()
        .expireAfterAccess(15, TimeUnit.MINUTES)
        .maximumSize(10000000)
        .build();
    static int buffsize = 5000000;

    /**
     * 在线车辆vid
     */
    private static final LinkedBlockingQueue<String> alives = new LinkedBlockingQueue<>(buffsize);

    /**
     * 在线车辆vid, 防重复
     */
    private final Set<String> aliveSet = new HashSet<>(buffsize / 5);

    static {
        setTime();
    }

    //    boolean cansend;
    {
        try {
            lastsendtime = 0L;
            carlasttimes = new HashMap<>();
            regHanler = new RegHanler();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean isNowSend() {
        if (oncesend <= 0) {
            return true;
        }

        long now = System.currentTimeMillis();
        if (now - lastsendtime > oncesend
            && now - lastsendtime <= 2 * oncesend) {//判断是否处于可推送状态
            return true;
        }
        if (now - lastsendtime > 2 * oncesend) {
            lastsendtime = now;
        }
        return false;
    }

    /**
     * 获取 carinfo中车辆的注册信息 是否可以监控并推送 es
     * 此方法在系统启动的时候调用一次
     *
     * @return
     */
    public List<Map<String, Object>> redisCarInfoSendMsgs() {
        try {
            if (carinfoSend) {
                return null;
            }

            carinfoSend = true;
            final Cache<String, String[]> carInfoCache = RedisClusterLoaderUseCtfo.getCarInfoCache();
            if (carInfoCache.size() > 1) {
                return getCarMonitorEsMsgs(carInfoCache.asMap());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * redis 集群中的数据一次全量推送通知，此方法只会调用一次
     *
     * @return
     */
    public List<Map<String, Object>> redisClusterSendMsgs() {
        if (clusterSend) {
            return null;
        }

        try {
            clusterSend = true;
            List<Map<String, Object>> msgs = null;
            Cache<String, Map<String, String>> redisMsgs = RedisClusterLoaderUseCtfo.getLastDataCache();
            if (redisMsgs.size() < 1) {
                return null;
            }
            LinkedBlockingQueue<String> carVids = RedisClusterLoaderUseCtfo.cachedVehicleIdQueue;
            Map<String, Map<String, String>> redismaps = redisMsgs.asMap();
            if (redismaps.size() < 1) {
                return null;
            }
            long now = System.currentTimeMillis();
            msgs = new LinkedList<>();
            String vid = carVids.poll();
            while (null != vid) {
                Map<String, String> map = redismaps.get(vid);
                if (map != null) {
                    Map<String, Object> msg = getSendEsMsgAndSetAliveLast(map, now);
                    if (null != msg && msg.size() > 0) {
                        msgs.add(msg);
                    }
                }
                vid = carVids.poll();
            }
            return msgs;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * <p>每调用一次此方法会批量的检查 所有在线的车辆 是否存在离线的情况
     * 离线处理：发送es离线消息，将其在在线的车辆缓存中移除
     *
     * </p>
     *
     * @return
     */
    public List<Map<String, Object>> checkAliveCarOffline() {
        if (statusAliveCars.size() > 0) {
            long now = System.currentTimeMillis();
            Map<String, Map<String, String>> cars = statusAliveCars.asMap();
            List<String> offlineKeys = new LinkedList<>();
            List<String> needListens = new LinkedList<>();
            String key = alives.poll();
            while (null != key) {
                aliveSet.remove(key);
                Map<String, String> dat = cars.get(key);
                if (null != dat) {

                    boolean online = isOnline(dat, now);
                    if (!online) {
                        offlineKeys.add(key);
                    } else {
                        needListens.add(key);
                    }
                }
                key = alives.poll();
            }

            if (needListens.size() > 0) {
                for (String vid : needListens) {
                    if (!aliveSet.contains(vid)) {
                        alives.offer(vid);
                        aliveSet.add(vid);
                    }
                }
            }

            if (offlineKeys.size() > 0) {
                List<Map<String, Object>> esMaps = new LinkedList<>();
                for (String vid : offlineKeys) {
                    //send offline msg
                    Map<String, Object> esmap = new TreeMap<>();
                    esmap.put(EsField.vid, vid);
                    esmap.put(EsField.serverTime, toEsDateString(new Date()));
                    esmap.put(EsField.carStatus, 0);
                    esmap.put(EsField.alarmStatus, 0);
                    esMaps.add(esmap);
                    //cache remove vid
                    cars.remove(vid);
                }
                return esMaps;
            }
        }
        return null;
    }

    public Map<String, Object> getSendEsMsgAndSetAliveLast(final Map<String, String> data, final long now) {

        // 更新车辆最新的报文时间
        setLast(data, now);

        // 更新最近活动的车辆
        setAliveCars(data, now);

        return getSendEsMsg(data, now);
    }

    public Map<String, Object> getRegCarMsg(Map<String, String> msg) {
        return regHanler.regHandler(msg);
    }

    /**
     * 获取carinfo 中可以监控的车辆
     *
     * @param map
     * @return
     */
    private List<Map<String, Object>> getCarMonitorEsMsgs(Map<String, String[]> map) {
        if (null == map || map.size() == 0) {
            return null;
        }
        try {
            List<Map<String, Object>> msgs = new LinkedList<>();
            for (Map.Entry<String, String[]> entry : map.entrySet()) {
                String key = entry.getKey();
                String[] strings = entry.getValue();

                if (StringUtils.isEmpty(key)
                    || ArrayUtils.isEmpty(strings)) {
                    continue;
                }

                if (strings.length != 15) {
                    continue;
                }
                String vid = strings[0];
                String monitor = strings[14];
                if (StringUtils.isEmpty(vid)
                    || StringUtils.isEmpty(monitor)) {
                    continue;
                }

                boolean ismonitor = "1".equals(monitor);
                Map<String, Object> esmap = new TreeMap<>();
                esmap.put(EsField.vid, vid);
                if (ismonitor) {
                    esmap.put(EsField.monitor, 1);
                } else {
                    esmap.put(EsField.monitor, 0);
                }
                msgs.add(esmap);
            }
            if (msgs.size() > 0) {
                return msgs;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * <p>
     * 数据可能是 心跳报文、登录登出报文、实时报文
     * 处理的时候需要注意
     * </p>
     *
     * @param data
     * @return
     */
    private Map<String, Object> getSendEsMsg(final Map<String, String> data, final long now) {
        if (null == data) {
            return null;
        }

        try {
            final String msgType = data.get(DataKey.MESSAGE_TYPE);
            final String vid = data.get(DataKey.VEHICLE_ID);
            final String time = data.get(DataKey.TIME);
            final Date date = inDate(time);
            if (StringUtils.isEmpty(msgType) || StringUtils.isEmpty(vid) || null == time || time.length() != 14 || null == date) {
                return null;
            }

            Map<String, Object> esmap = new TreeMap<>();
            esmap.put(EsField.vid, vid);

            if (CommandType.SUBMIT_REALTIME.equals(msgType)) {
                // 实时报文
                esDat(esmap, data, time, now);
            }
            // 终端注册消息
            else if (CommandType.SUBMIT_LOGIN.equals(msgType)) {

                // 平台注册通知类型
                final String regType = data.get(ProtocolItem.REG_TYPE);

                // 车机终端上线
                if ("1".equals(regType)) {
                    esmap.put(EsField.carStatus, 1);
                    esmap.put(EsField.onlineStatus, 2);
                    esmap.put(EsField.serverTime, toEsDateString(new Date()));
                    esmap.put(EsField.terminalTime, toTimeString(time));
                }
                // 车机离线
                else if ("2".equals(regType)) {
                    esmap.put(EsField.carStatus, 0);
                    esmap.put(EsField.onlineStatus, 0);
                    esmap.put(EsField.serverTime, toEsDateString(new Date()));
                    esmap.put(EsField.terminalTime, toTimeString(time));
                }
            }
            // 链接状态通知
            else if (CommandType.SUBMIT_LINKSTATUS.equals(msgType)) {
                String linkType = data.get(SUBMIT_LINKSTATUS.LINK_TYPE);
                boolean isValid = false;
                if (SUBMIT_LINKSTATUS.isOnlineNotice(linkType)
                    || SUBMIT_LINKSTATUS.isHeartbeatNotice(linkType)) {
                    esmap.put(EsField.carStatus, 1);
                    esmap.put(EsField.onlineStatus, 2);
                    isValid = true;
                } else if (SUBMIT_LINKSTATUS.isOfflineNotice(linkType)) {
                    esmap.put(EsField.carStatus, 0);
                    esmap.put(EsField.alarmStatus, 0);
                    isValid = true;
                }
                if (isValid) {
                    esmap.put(EsField.serverTime, toEsDateString(new Date()));
                    esmap.put(EsField.terminalTime, toTimeString(time));
                }
            }
            else if (CommandType.SUBMIT_TERMSTATUS.equals(msgType)) {
            }
            else if (CommandType.SUBMIT_HISTORY.equals(msgType)) {
            }
            else if (CommandType.SUBMIT_CARSTATUS.equals(msgType)) {
            }
            // 租赁数据
            else if (SysDefine.RENTCAR.equals(msgType)) {
            }
            // 充电设施数据
            else if (SysDefine.CHARGE.equals(msgType)) {
            }

            if (esmap.size() > 1) {
                esmap.put(EsField.monitor, 1);
                return esmap;
            }
        } catch (Exception e) {
            LOG.warn("", e);
        }

        return null;
    }

    /**
     * <p>
     * 这个方法需要注意！！！
     * </p>
     *
     * @param esMap 通知es的
     * @param data   报文内部协议解析后的数据
     * @param time  报文终端时间
     * @param now   现在的UTC时间
     */
    private void esDat(final Map<String, Object> esMap, final Map<String, String> data, final String time, final long now) {
        String tenthKm = data.get(DataKey._2202_TOTAL_MILEAGE);
        String longitude = data.get(DataKey._2502_LONGITUDE);
        String latitude = data.get(DataKey._2503_LATITUDE);
        String orientation = data.get(DataKey._2501_ORIENTATION);

        char[] oris = toBinaryCharArr(orientation);
        if (null != oris && '0' == oris[3]
            && !StringUtils.isEmpty(longitude)
            && !StringUtils.isEmpty(latitude)) {
            double longit = Double.valueOf(longitude);
            double latitu = Double.valueOf(latitude);
            longit = longit / 1000000.0;
            latitu = latitu / 1000000.0;

            if (longit <= 180 && latitu <= 90) {

                if ('1' == oris[2]) {
                    latitu = -latitu;
                }
                if ('1' == oris[1]) {
                    longit = -longit;
                }
                String location = latitu + "," + longit;
                esMap.put(EsField.location, location);
                esMap.put(EsField.gpsValueValid, 0);
            }
            esMap.put(EsField.gpsValid, 0);
        } else {
            esMap.put(EsField.gpsValid, 1);
        }

        if (data.containsKey(SysDefine.ONLINE_UTC)) {

            long lastTime = Long.valueOf(data.get(SysDefine.ONLINE_UTC));
            if (now - lastTime < onlinetime) {
                esMap.put(EsField.carStatus, 1);
                boolean isstop = isStop(data);
                if (isstop) {
                    esMap.put(EsField.onlineStatus, 3);
                } else {
                    esMap.put(EsField.onlineStatus, 2);
                }
            } else {
                esMap.put(EsField.carStatus, 0);
            }
        }

        if (data.containsKey(DataKey._2301_CHARGE_STATUS)) {
            String chargeStatus = data.get(DataKey._2301_CHARGE_STATUS);
            if (null != chargeStatus && !"".equals(chargeStatus.trim())
                && !"255".equals(chargeStatus)
                && !"254".equals(chargeStatus)) {

                esMap.put(EsField.chargeStatus, Integer.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(chargeStatus) ? chargeStatus : "0"));
            }
        }
        if ("1".equals(data.get(SysDefine.IS_ALARM)) && null != data.get(SysDefine.ALARMUTC)) {
            esMap.put(EsField.alarmStatus, 2);
            esMap.put(EsField.alarmTime, toEsDateString(data.get(SysDefine.ALARMUTC)));
        } else {
            esMap.put(EsField.alarmStatus, 0);
        }
        esMap.put(EsField.serverTime, toEsDateString(new Date()));
        esMap.put(EsField.terminalTime, toTimeString(time));

        tenthKm = org.apache.commons.lang.math.NumberUtils.isNumber(tenthKm) ? tenthKm : "0";
        if (!"0".equals(tenthKm)) {
            double km = Double.parseDouble(tenthKm);
            if (km < 10000000) {
                esMap.put(EsField.tenthKm, km);
            }
        }
    }

    private static void setTime() {
        try {
            Properties pties = CONFIG_UTILS.sysDefine;
            if (null != pties) {
                String oncetime = pties.getProperty(SysDefine.ES_SEND_TIME);
                if (null != oncetime) {
                    oncesend = Long.valueOf(oncetime) * 1000;
                }
                String offli = pties.getProperty(StormConfigKey.REDIS_OFFLINE_SECOND);
                if (null != offli) {
                    onlinetime = 1000 * Long.valueOf(offli);
                }
                String stopli = pties.getProperty("redis.offline.stoptime");
                if (null != stopli) {
                    stoptime = 1000 * Long.valueOf(stopli);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新车辆最新的报文时间
     *
     * @param data
     * @return
     */
    private boolean setLast(final Map<String, String> data, final long now) {
        if (null == data) {
            return false;
        }
        try {
            final String msgType = data.get(DataKey.MESSAGE_TYPE);
            final String vid = data.get(DataKey.VEHICLE_ID);
            final String time = data.get(DataKey.TIME);
            if (StringUtils.isEmpty(msgType)
                || StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)) {
                return false;
            }
            long lastTime = 0L;
            if (data.containsKey(SysDefine.ONLINE_UTC)) {
                lastTime = Long.valueOf(data.get(SysDefine.ONLINE_UTC));
            } else {
                Date date = inDate(time);
                if (null != date) {
                    lastTime = date.getTime();
                }
            }

            //最后一条报文时间小于当前系统时间 + 30秒的误差
            if (lastTime > 0 && lastTime < now + 30000) {
                boolean islast = true;
                if (carlasttimes.containsKey(vid)) {
                    long prev = carlasttimes.get(vid);
                    if (lastTime < prev) {
                        islast = false;
                    }
                }
                if (islast) {
                    carlasttimes.put(vid, lastTime);
                    return true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 最近 onlinetime 毫秒内的车辆报文加入到 statusAliveCars 中
     *
     * @param data
     * @param now
     * @return
     */
    private boolean setAliveCars(final Map<String, String> data, final long now) {
        if (null == data) {
            return false;
        }
        try {
            final String msgType = data.get(DataKey.MESSAGE_TYPE);
            final String vid = data.get(DataKey.VEHICLE_ID);
            final String time = data.get(DataKey.TIME);
            if (StringUtils.isEmpty(msgType)
                || StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)) {
                return false;
            }

            long lastTime = 0L;
            if (data.containsKey(SysDefine.ONLINE_UTC)) {
                lastTime = Long.valueOf(data.get(SysDefine.ONLINE_UTC));
            } else {
                Date date = inDate(time);
                if (null != date) {
                    lastTime = date.getTime();
                    data.put(SysDefine.ONLINE_UTC, "" + lastTime);
                }
            }
            if (lastTime > 0) {
                //最后一条报文时间小于当前系统时间 + 30秒的误差
                if (now - lastTime <= onlinetime && lastTime < now + 30000) {
                    if (!aliveSet.contains(vid)) {
                        alives.offer(vid);
                        aliveSet.add(vid);
                    }
                    statusAliveCars.put(vid, data);
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.warn("更新车辆状态缓存异常.", e);
        }
        return false;
    }

    /**
     * 判断当前车辆报文的时间是否处于在线状态
     *
     * @param dat 报文数据
     * @param now 系统现在时间
     * @return
     */
    private boolean isOnline(Map<String, String> dat, long now) {
        if (null == dat) {
            return false;
        }
        try {
            String msgType = dat.get(DataKey.MESSAGE_TYPE);
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            if (StringUtils.isEmpty(msgType)
                || StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)) {
                return false;
            }
            if (CommandType.SUBMIT_LOGIN.equals(msgType)
                && dat.containsKey(SUBMIT_LOGIN.LOGOUT_TIME)) {//离线

                if (!dat.containsKey(SUBMIT_LOGIN.LOGIN_TIME)) {
                    return true;
                } else {
                    String logintime = dat.get(SUBMIT_LOGIN.LOGIN_TIME);
                    String logouttime = dat.get(SUBMIT_LOGIN.LOGOUT_TIME);
                    if (null == logintime) {
                        return true;
                    } else {
                        if (null != logouttime) {
                            long outtime = Long.parseLong(logouttime);
                            long intime = Long.parseLong(logintime);

                            if (outtime > intime) {
                                return true;
                            }
                        }
                    }
                }
            } else if (CommandType.SUBMIT_LINKSTATUS.equals(msgType)
                && "3".equals(dat.get("TYPE"))) {
                //离线
                return true;
            }
            long lastTime = -1;
            if (dat.containsKey(SysDefine.ONLINE_UTC)) {
                lastTime = Long.valueOf(dat.get(SysDefine.ONLINE_UTC));

            } else {
                Date date = inDate(time);
                if (null != date) {
                    lastTime = date.getTime();
                }
            }
            if (now - lastTime <= onlinetime) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    boolean isStop(Map<String, String> map) {
        try {
            String vid = map.get(DataKey.VEHICLE_ID);
            String rev = map.get(DataKey._2303_DRIVING_ELE_MAC_REV);
            String spd = map.get(DataKey._2201_SPEED);
            if (!"0".equals(spd) || !"20000".equals(rev)) {
                zeroCache.remove(vid);
                return false;
            }
            if ("0".equals(spd) && "20000".equals(rev)) {
                String timelong = map.get(SysDefine.ONLINE_UTC);
                String lon = map.get(DataKey._2502_LONGITUDE);//经度
                String lan = map.get(DataKey._2503_LATITUDE);//纬度

                Map<String, String> startZero = zeroCache.get(vid);
                if (null == startZero) {
                    startZero = new TreeMap<>();
                    startZero.put(DataKey._2303_DRIVING_ELE_MAC_REV, rev);
                    startZero.put(DataKey._2201_SPEED, spd);
                    startZero.put(DataKey._2502_LONGITUDE, lon);
                    startZero.put(DataKey._2503_LATITUDE, lan);
                    startZero.put(SysDefine.ONLINE_UTC, timelong);

                    zeroCache.put(vid, startZero);
                    return false;
                } else {
                    long lastTime = Long.valueOf(map.get(SysDefine.ONLINE_UTC));
                    long starttime = Long.valueOf(startZero.get(SysDefine.ONLINE_UTC));
                    if (lastTime - starttime >= stoptime) {
                        String slon = startZero.get(DataKey._2502_LONGITUDE);//经度
                        String slan = startZero.get(DataKey._2503_LATITUDE);//纬度
                        if ((StringUtils.isEmpty(lon)
                            || StringUtils.isEmpty(lan))
                            && (StringUtils.isEmpty(slon)
                            || StringUtils.isEmpty(slan))) {
                            return true;
                        }
                        if (!StringUtils.isEmpty(lon)
                            && !StringUtils.isEmpty(lan)
                            && !StringUtils.isEmpty(slon)
                            && !StringUtils.isEmpty(slan)) {
                            long longi = Long.valueOf(lon);
                            long slongi = Long.valueOf(slon);
                            long lati = Long.valueOf(lan);
                            long slati = Long.valueOf(slan);
                            if (Math.abs(longi - slongi) <= 2
                                && Math.abs(lati - slati) <= 2) {
                                return true;
                            }
                        }

                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    private char[] toBinaryCharArr(String vl) {//String 类型的值v>=0,v<=8
        if (StringUtils.isEmpty(vl)) {
            return null;
        }
        String binaryString = toBinary(vl);
        if (4 < binaryString.length()) {
            binaryString = binaryString.substring(binaryString.length() - 4);
        }
        return binaryString.toCharArray();//{'1','0','0','0'}
    }

    private String toBinary(String vl) {//String 类型的值v>=0,v<=8
        if (null == vl) {
            return "0001";
        }
        try {
            int v = Integer.valueOf(vl);
            String string = Integer.toBinaryString(v);
            if (1 == string.length()) {
                return "000" + string;
            } else if (2 == string.length()) {
                return "00" + string;
            } else if (3 == string.length()) {
                return "0" + string;
            } else if (4 == string.length()) {
                return string;
            }
            return string;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "0001";
    }

    private String toTimeString(String intime) {//yyyyMMddHHmmss
        if (null == intime || intime.length() != 14) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        builder.append(intime.substring(0, 4)).append("-")
            .append(intime.substring(4, 6)).append("-")
            .append(intime.substring(6, 8)).append(" ")
            .append(intime.substring(8, 10)).append(":")
            .append(intime.substring(10, 12)).append(":")
            .append(intime.substring(12));
        intime = null;
        return builder.toString();//yyyy-MM-dd HH:mm:ss
    }

    private String toEsDateString(Date date) {
        if (null == date) {
            return null;
        }
        SimpleDateFormat format = getOutDateFormat();
        return format.format(date);
    }

    private String toEsDateString(String utc) {//long 时间
        if (null == utc || "".equals(utc.trim())) {
            return null;
        }
        try {
            long time = Long.valueOf(utc);
            String estime = toEsDateString(new Date(time));
            return estime;
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将字符串格式时间转成Date对象
     * @param time
     * @return
     */
    private Date inDate(String time) {//yyyyMMddHHmmss
        if (null == time || "".equals(time.trim())) {
            return null;
        }

        SimpleDateFormat format = getInDateFormat();
        try {
            return format.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    private SimpleDateFormat getInDateFormat() {
        SimpleDateFormat format = inFormatlocal.get();
        if (null == format) {
            format = new SimpleDateFormat("yyyyMMddHHmmss");
            inFormatlocal.set(format);
        }
        return inFormatlocal.get();
    }

    private SimpleDateFormat getOutDateFormat() {
        SimpleDateFormat format = outFormatlocal.get();
        if (null == format) {
            format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            outFormatlocal.set(format);
        }
        return outFormatlocal.get();
    }

}

class EsField implements java.io.Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 2300001L;

    public static String vid = "uuid";
    public static String serverTime = "serverTime";
    public static String terminalTime = "terminalTime";
    public static String alarmTime = "alarmTime";
    public static String faultTime = "faultTime";
    public static String chargeStatus = "chargeStatus";
    public static String monitor = "monitor";
    public static String carStatus = "carStatus";
    public static String onlineStatus = "onlineStatus";
    public static String alarmStatus = "alarmStatus";
    public static String tenthKm = "tenthKm";
    public static String location = "location";
    public static String speed = "speed";
    public static String iccid = "iccid";
    public static String gpsValid = "gpsValid";//gps 有效只要有值，根据报文中0有效 1无效而来
    public static String gpsValueValid = "gpsValueValid";//gps 数值有效性，在 +-90,+-180之间
    public static String status = "status";
}
