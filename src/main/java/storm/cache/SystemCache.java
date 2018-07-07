package storm.cache;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ctfo.datacenter.cache.handle.CTFOCacheKeys;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import storm.dao.DataToRedis;
import storm.system.DataKey;
import storm.util.CTFOUtils;
import storm.util.ConfigUtils;
import storm.util.NumberUtils;
import storm.system.SysDefine;

public class SystemCache {
    private static final ConfigUtils configUtils = ConfigUtils.getInstance();
    public static Cache<String, Map<String,String>> newDefaultCache(){
        return CacheBuilder.newBuilder()
                .expireAfterAccess(10,TimeUnit.MINUTES)
                .maximumSize(10000)
                .build();
    }
    private static Map<String, String> filterRules;
    private static Map<String, Set<String>> alarmRules;
    private static Set<String> defaultAlarmRules;
    private static DataToRedis redis;
    private static final long time = 3 * 60 * 1000 ;
    static{
        redis=new DataToRedis();
        initRules(redis);
        tasks();
    }

    public static void init(){

    }
    private static void tasks(){
        ScheduledExecutorService service ;
//        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new ListenThread(), 0, Long.parseLong(ConfigUtils.getInstance().sysDefine.getProperty("redis.listenInterval")), TimeUnit.SECONDS);
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new RebulidThread(), 0, Long.parseLong(ConfigUtils.getInstance().sysDefine.getProperty("redis.timeInterval")), TimeUnit.SECONDS);
    }
    private static void initRules(DataToRedis r){
        filterRules=r.getFilterMap();
        alarmRules=r.getAlarmMap();
        defaultAlarmRules=r.smembers(4, "XNY.ALARM_ALL");
    }
    private static void destoryRules(){
        if(null != filterRules) {
            filterRules.clear();
        }
        if(null != alarmRules) {
            alarmRules.clear();
        }
        if (null != defaultAlarmRules) {
            defaultAlarmRules.clear();
        }
        filterRules=null;
        alarmRules=null;
        defaultAlarmRules=null;
    }
    private static void rebuild(DataToRedis r){
        destoryRules();
        if(null == r) {
            r=new DataToRedis();
        }
        initRules(r);
    }
    public static Map<String, String> filterRule(){
        if(null == filterRules) {
            initRules(redis);
        }
        return filterRules;
    }

    public static Map<String, Set<String>> alarmRule(){
        if(null == alarmRules) {
            initRules(redis);
        }
        return alarmRules;
    }
    public static Set<String> defaultAlarmRule(){
        if(null == defaultAlarmRules) {
            initRules(redis);
        }
        return defaultAlarmRules;
    }
    /** 当日最大里程值 */
    public static final Map<String, Long> MAX_MILE_MAP = new ConcurrentHashMap<String, Long>();
    /** 当日最小里程值 */
    public static final Map<String, Long> MIN_MILE_MAP = new ConcurrentHashMap<String, Long>();
    public static final Map<String, Integer> CHARGE_MAP = new ConcurrentHashMap<String, Integer>();
    public static final Map<String, String> ALARM_MAP= new ConcurrentHashMap<String,String>();
    public static final Map<String, String> VEH_DATA_MAP= new ConcurrentHashMap<String,String>();
    public static final Map<String, String> VID2_ALARM = new ConcurrentHashMap<String, String>();//车辆报警信息缓存(vid----是否报警_最后报警时间)
    public static final Map<String, String> VID2_ALARM_END = new ConcurrentHashMap<String, String>();//车辆报警信息缓存(vid----报警结束次数)
    public static final Map<String, String> VID2_ALARM_INFO = new ConcurrentHashMap<String, String>();//预警判断信息缓存(vid-filterid-----报警次数_连续报警持续时间_最近一次报警时刻)
    
    private static void protectMachine(){
        if(MAX_MILE_MAP.size()>2000000) {
            MAX_MILE_MAP.clear();
        }
        if(MIN_MILE_MAP.size()>2000000) {
            MIN_MILE_MAP.clear();
        }
        if(CHARGE_MAP.size()>2000000) {
            CHARGE_MAP.clear();
        }
        if(ALARM_MAP.size()>2000000) {
            ALARM_MAP.clear();
        }
        if(VEH_DATA_MAP.size()>2000000) {
            VEH_DATA_MAP.clear();
        }
        if(VID2_ALARM.size()>2000000) {
            VID2_ALARM.clear();
        }
        if(VID2_ALARM_END.size()>2000000) {
            VID2_ALARM_END.clear();
        }
    }
    static class RebulidThread implements Runnable{

        @Override
        public void run() {
            rebuild(redis);
            protectMachine();
        }

    }
    static class ListenThread implements Runnable{

        @Override
        public void run() {
            try {
                Map<String,String> map = new TreeMap<String, String>();
                for(int i = 0;i<2;i++){
                    String dataId = "";
                    String utcId = "";
                    String statisId = "";

                    if( i == 0){
                        dataId = SysDefine.ISONLINE;
                        utcId = SysDefine.ONLINEUTC;
                        statisId = SysDefine.ONLINE_COUNT;
                    }else{
                        dataId = SysDefine.ISALARM;
                        utcId = SysDefine.ALARMUTC;
                        statisId = SysDefine.FAULT_COUNT;
                    }

                    List<String> vid2utc = getVehicleStatusList(dataId,utcId);
                    int size = 0;
                    int count = 0;

                    if (!CollectionUtils.isEmpty(vid2utc)) {
                        size = vid2utc.size();
                        List<String> vidList = new LinkedList<String>();
                        for(String value : vid2utc){
                            if (!StringUtils.isEmpty(value)) {
                                long utc = Long.parseLong(value.split(":")[1]);
                                if(System.currentTimeMillis() - utc >= time){
                                    String string=value.split(":")[0];
                                    if (!StringUtils.isEmpty(string)) {
                                        vidList.add(new String(string));
                                        count++;
                                    }
                                }
                            }
                            
                        }

                        for(String vid : vidList){
                            if (!StringUtils.isEmpty(vid)) {
                                try {
                                    CTFOUtils.getDefaultCTFOCacheTable().addHash(vid, dataId, "0");
                                } catch (Exception e) {
                                    System.out.println("自动判断" + dataId + "存储redis异常！" + e);
                                }
                            }
                        }
                        vidList=null;
                        vid2utc.clear();
                    }

                    map.put(statisId, size -count+"");
                }

                map.put(SysDefine.MILEAGE_TOTAL,getVehicleMileage());
                String vehicle_total = redis.getValueByDataId("TOTAL_DATA", SysDefine.VEHICLE_TOTAL);
                if(null == vehicle_total || "".equals(vehicle_total) || "0".equals(vehicle_total)){
                    map.put(SysDefine.ONLINE_RATIO, "0");
                }else {
                    long onlineCount = Long.parseLong(map.get(SysDefine.ONLINE_COUNT));
                    map.put(SysDefine.ONLINE_RATIO, Math.round((double)onlineCount / Double.parseDouble(vehicle_total) *100)/100.0 +"");
                }
                redis.saveStatisticsMessage(map);

            } catch (Exception e) {
                System.out.println("------临时统计任务异常！" + e);
            }
        }

        private List<String> getVehicleStatusList(String dataId, String utcId) {
            List<String> list = new LinkedList<String>();
            CTFOCacheKeys ctfoCacheKeys;
            try {
                ctfoCacheKeys = CTFOUtils.getDefaultCTFOCacheTable().getCTFOCacheKeys();
                while(ctfoCacheKeys.next()){
                    List<String>keys=ctfoCacheKeys.getKeys();
                    for(String key :keys){
                        if(!StringUtils.isEmpty(key)){
                            key = key.split("-",3)[2];
                            List<String> value = CTFOUtils.getDefaultCTFOCacheTable().queryHash(key, dataId, utcId);
                            if(!CollectionUtils.isEmpty(value)
                                    && "1".equals(value.get(0))
                                    && value.size()>=2 ){
                                list.add(new String(key+":"+value.get(1)));
                            }
                        }
                        
                    }
                }
            } catch (Exception e) {
                System.out.println("--------redis实时数据ONLINE遍历异常！" + e);
            }
            if (CollectionUtils.isEmpty(list)) {
                return null;
            }
            return list;
        }

        private String getVehicleMileage() {
            double mileage = 0;
            CTFOCacheKeys ctfoCacheKeys;
            try {
                ctfoCacheKeys = CTFOUtils.getDefaultCTFOCacheTable().getCTFOCacheKeys();
                while(ctfoCacheKeys.next()){
                    List<String>keys=ctfoCacheKeys.getKeys();
                    for(String key :keys){
                        if (!StringUtils.isEmpty(key)) {
                            String []keyarr=key.split("-",3);
                            if (keyarr.length>=3) {
                                key = keyarr[2];
                                if (!StringUtils.isEmpty(key)) {
                                    mileage = mileage + Double.parseDouble(NumberUtils.stringNumber((CTFOUtils.getDefaultCTFOCacheTable().queryHash(key, DataKey._2202_TOTAL_MILEAGE))));
                                }
                            }

                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("--------redis实时数据计算总里程异常！" + e);
                e.printStackTrace();
                return "";
            }

            return mileage+"";
        }

    }
}
