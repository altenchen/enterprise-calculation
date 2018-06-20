package storm.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import storm.dao.DataToRedis;
import storm.dto.FillChargeCar;
import storm.handler.cal.RedisClusterLoaderUseCtfo;
import storm.service.TimeFormatService;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.NumberUtils;
import storm.util.ObjectUtils;
import storm.util.ParamsRedisUtil;

/**
 * 系统实时数据缓存
 * @author 76304
 *
 */
public class SysRealDataCache {

	private static Logger logger = LoggerFactory.getLogger(SysRealDataCache.class);
	private static final ConfigUtils configUtils = ConfigUtils.getInstance();
	public static final String unknow="UNKNOW";
	/**
	 * 缓存666天, 最多1500万条
	 */
	private static Cache<String,Map<String,String>> carlastrecord = CacheBuilder.newBuilder()
			.expireAfterAccess(666,TimeUnit.DAYS)
			.maximumSize(15000000)
			.build();
	/**
	 * 缓存60分钟, 最多1500万条
	 */
	private static Cache<String, String[]>carInfoCache = CacheBuilder.newBuilder()
			.expireAfterAccess(60,TimeUnit.MINUTES)
			.maximumSize(15000000)
			.build();
	/**
	 * 缓存30天, 最多1000万条
	 */
	public static Cache<String,Map<String,String>> livelyCarCache = CacheBuilder.newBuilder()
			.expireAfterAccess(30,TimeUnit.DAYS)
			.maximumSize(10000000)
			.build();
	private static Map<String,FillChargeCar> chargeCarCache;
	private static DataToRedis redis = new DataToRedis();
	public static final String [] unknowArray =new String[]{"UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW"};
	private static long lasttime;
	private static long flushtime = 2100000;//2100秒 35分钟刷新一下
	private static List<String> chargeTypes;
	static TimeFormatService timeformat;
	static long timeouttime = 86400000L;

    /**
     * 缓冲窗口大小
     */
	private static final int buffsize = 5000000;
	/**
	 *
	 */
	public static LinkedBlockingQueue<String> alives = new LinkedBlockingQueue<>(buffsize);
    /**
     *
     */
	static Set<String> aliveSet = new HashSet<>(buffsize/5);

    /**
     *
     */
	public static LinkedBlockingQueue<String> lasts = new LinkedBlockingQueue<>(buffsize);
    /**
     *
     */
	static Set<String> lastSet = new HashSet<>(buffsize/5);

	static {
		try {
            carInfoCache = RedisClusterLoaderUseCtfo.getCarinfoCache();
			carlastrecord = RedisClusterLoaderUseCtfo.getDataCache();
			long now = System.currentTimeMillis();
			lasttime = now;
			chargeCarCache = new ConcurrentHashMap<String,FillChargeCar>();
			timeformat = TimeFormatService.getInstance();
			
	        Object outbyconf = ParamsRedisUtil.getInstance().PARAMS.get("gt.inidle.timeOut.time");
			if (!ObjectUtils.isNullOrEmpty(outbyconf)) {
				timeouttime=1000*(int)outbyconf;
			}
			if (null != configUtils.sysParams) {
				
				String typeparams = configUtils.sysParams.getProperty("charge.car.type.id");
				if (!ObjectUtils.isNullOrEmpty(typeparams)) {
					int colidx = typeparams.indexOf(",");
					String []strings = null;
					if (colidx > 0) {
						strings = typeparams.split(",");
					} else {
						strings = new String[]{typeparams};
					}
					if (null != strings) {
						chargeTypes = new ArrayList<String>(strings.length);
						for (int i = 0; i < strings.length; i++) {
							if(!ObjectUtils.isNullOrEmpty(strings[i])
									&& !chargeTypes.contains(strings[i])){
								chargeTypes.add(new String(strings[i]));
							}
						}
					}
				} else {
					chargeTypes = new ArrayList<String>(1);
					chargeTypes.add("402894605f511508015f516968890198");
				}
			} else {
				chargeTypes = new ArrayList<String>(1);
				chargeTypes.add("402894605f511508015f516968890198");
			}
			initChargeCarCache(now);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		
	}
	
	public static String[] carInfoByVin(final String vin){
		String []carArr=unknowArray;
		try {
			carArr=getCarinfoCache().get(vin, new Callable<String[]>() {

				@Override
				public String[] call() throws Exception {
					
					return unknowArray;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		return carArr;
	}
	public static Cache<String, String[]> getCarinfoCache(){
		long now = System.currentTimeMillis();
		if (now -lasttime > flushtime) {
			lasttime = now;
			resetCarCache();
		}
		return carInfoCache;
	}

	public static Cache<String,Map<String,String>> getDataCache(){
		
		return carlastrecord;
	}
	
	public static Cache<String,Map<String,String>> getLivelyCache(){
		
		return livelyCarCache;
	}
	
	public static Map<String,FillChargeCar> chargeCars(){
		return chargeCarCache;
	}
	
	public static void updateCache(Map<String, String> dat, long now){
		addChargeCar(dat);
		addCarCache(dat);
		addLivelyCar(dat, now, timeouttime);
	}
	private static void addChargeCar(Map<String, String> dat){
		if (null == dat || dat.size() ==0) {
			return;
		}
		if ( !dat.containsKey(DataKey.VEHICLE_ID)
				|| !dat.containsKey(DataKey.VEHICLE_NUMBER)) {
			return;
		}
		String vid = dat.get(DataKey.VEHICLE_ID);
		String vin = dat.get(DataKey.VEHICLE_NUMBER);
		String[] strings = carInfoByVin(vin);
		if(null ==strings || strings.length != 15) {
			return ;
		}
		String cartypeId = strings[10];
		if (null == cartypeId || unknow.equals(cartypeId)) {
			return;
		}
		if (chargeTypes.contains(cartypeId.trim())) {
			String time = dat.get(DataKey.TIME);
			String latit = dat.get(DataKey._2503_LATITUDE);
			String longi = dat.get(DataKey._2502_LONGITUDE);
			
			if (! ObjectUtils.isNullOrEmpty(time)
					&& ! ObjectUtils.isNullOrEmpty(latit)
					&& ! ObjectUtils.isNullOrEmpty(longi)) {
				double longitude = Double.parseDouble(NumberUtils.stringNumber(longi));
				double latitude = Double.parseDouble(NumberUtils.stringNumber(latit));
				longitude = longitude/1000000.0;
				latitude = latitude/1000000.0;
				FillChargeCar chargeCar = new FillChargeCar(vid, longitude, latitude, time);
				chargeCarCache.put(vid, chargeCar);
			}
		}
	}
	
	private static void addCarCache(Map<String, String> dat){
		if (null == dat || dat.size() ==0) {
			return;
		}
		if (!dat.containsKey(DataKey.VEHICLE_ID)) {
			return;
		}
		try {
			Map<String, String> newmap =  new TreeMap<String, String>();
			//不缓存无用的数据项，减小缓存大小
			for (Map.Entry<String, String> entry : dat.entrySet()) {
				String mapkey=entry.getKey();
				String value=entry.getValue();
				if (null!= mapkey && null !=value 
						&& !mapkey.startsWith("useful")
						&& !mapkey.startsWith("newest")
						&& !"2001".equals(mapkey)
						&& !"2002".equals(mapkey)
						&& !"2003".equals(mapkey)
						&& !"2101".equals(mapkey)
						&& !"2103".equals(mapkey)
						&& !"7001".equals(mapkey)
						&& !"7003".equals(mapkey)
						&& !"7101".equals(mapkey)
						&& !"7103".equals(mapkey)) {
					newmap.put(mapkey, value);
				}
			}
			String vid = newmap.get(DataKey.VEHICLE_ID);
			carlastrecord.put(vid, newmap);
			addLastQueue(vid);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 最近 onlinetime 毫秒内的车辆报文加入到 statusAliveCars 中
	 * @param dat
	 * @param now
	 * @return
	 */
	private static boolean addLivelyCar(Map<String, String> dat,long now,long timeout){
		if(null == dat) {
			return false;
		}
		try {
			String msgType = dat.get(SysDefine.MESSAGETYPE);
			String vid = dat.get(DataKey.VEHICLE_ID);
			String time = dat.get(SysDefine.TIME);
			if(ObjectUtils.isNullOrEmpty(msgType)
					|| ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)) {
				return false;
			}
			
			String utc = dat.get(SysDefine.ONLINEUTC);
			long utctime = Long.valueOf(NumberUtils.stringNumber(utc));
			long tertime = timeformat.stringTimeLong(time);
			long lastTime = Math.max(utctime, tertime);
			if (! dat.containsKey(SysDefine.ONLINEUTC)) {
				dat.put(SysDefine.ONLINEUTC, ""+lastTime);
			} 
			if(lastTime>0){
				if (now-lastTime <= timeout ){//最后一条报文时间小于当前系统时间 + 30秒的误差
					addAliveQueue(vid);
					livelyCarCache.put(vid, dat);
					return true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public static void addLastQueue(String vid){
		if (!lastSet.contains(vid)) {
			lasts.offer(vid);
			lastSet.add(vid);
		}
	}
	
	public static void removeLastQueue(String vid){
		if (lastSet.contains(vid)) {
			lastSet.remove(vid);
		}
	}
	
	public static void addAliveQueue(String vid){
		if (!aliveSet.contains(vid)) {
			alives.offer(vid);
			aliveSet.add(vid);
		}
	}
	
	public static void removeAliveQueue(String vid){
		if (aliveSet.contains(vid)) {
			aliveSet.remove(vid);
		}
	}
	
	/**
	 * 判断当前车辆报文的时间是否处于 自定义的超时状态
	 * @param dat 报文数据
	 * @param now 系统现在时间
	 * @return
	 */
	boolean istimeout(Map<String, String> dat,long now,long timeout){
		if(null == dat) {
			return false;
		}
		try {
			String msgType = dat.get(SysDefine.MESSAGETYPE);
			String vid = dat.get(DataKey.VEHICLE_ID);
			String time = dat.get(SysDefine.TIME);
			if(ObjectUtils.isNullOrEmpty(msgType)
					|| ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)) {
				return false;
			}
			if (dat.containsKey(SysDefine.ONLINEUTC)) {
				long lastTime=Long.valueOf(dat.get(SysDefine.ONLINEUTC));
				if (now-lastTime <= timeout) {
					return true;
				}
			} else {
				long lastTime = timeformat.stringTimeLong(time);
				if (now-lastTime<= timeout) {
					return true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	private static void resetCarCache(){
		if (null != carInfoCache) {
			carInfoCache.cleanUp();
		}
		Map<String, String> map = redis.hgetallMapByKeyAndDb("XNY.CARINFO", 0);
		for (Map.Entry<String, String> entry : map.entrySet()) {
			try {
				String key = entry.getKey();
				String value = entry.getValue();
				
				if (ObjectUtils.isNullOrEmpty(key)
						|| ObjectUtils.isNullOrEmpty(value)) {
					continue;
				}

				String []strings=value.split(",",-1);
				
				if(strings.length != 15) {
					continue;
				}
				carInfoCache.put(key, strings);
			}catch (Exception e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
	}
	
	private static void initChargeCarCache(long now ){
		Map<String,Map<String,String>> cluster=getDataCache().asMap();
		for (Map.Entry<String,Map<String,String>> entry : cluster.entrySet()) {
			Map<String,String> dat = entry.getValue();
			addChargeCar(dat);
			addLivelyCar(dat, now, timeouttime);
		}
	}
	public static void init(){}
}
