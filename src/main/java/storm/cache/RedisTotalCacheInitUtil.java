package storm.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctfo.datacenter.cache.handle.CTFOCacheKeys;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import storm.cache.util.RedisOrganizationUtil;
import storm.dao.DataToRedis;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.CTFOUtils;
import storm.util.ConfigUtils;
import storm.util.NumberUtils;
import storm.util.ObjectUtils;

public class RedisTotalCacheInitUtil {
	private static Logger logger = LoggerFactory.getLogger(RedisTotalCacheInitUtil.class);
	public static final String unknow="UNKNOW";
	public static final String unknowAndunknow="UNKNOW,UNKNOW";
	public static final String unknowString="UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW";
	public static final String [] unknowArray =new String[]{"UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW"};
	private static DataToRedis redis;
	private static long time = 180 * 1000 ;
	private static long stoptime = 180 * 1000 ;
	public static final long standard = 1510156800000L;//1510156800000L;//2017-11-09 00:00:00
	public static Cache<String,String> carInfoCache = CacheBuilder.newBuilder()
			.expireAfterAccess(60,TimeUnit.MINUTES)
			.maximumSize(5000000)
			.build();
	
	public static Cache<String,Set<String>> carUserCache = CacheBuilder.newBuilder()
			.expireAfterAccess(60,TimeUnit.MINUTES)
			.maximumSize(1000000)
			.build();
	
	public static Cache<String,String> districtParentCache = CacheBuilder.newBuilder()
			.expireAfterAccess(60,TimeUnit.MINUTES)
			.maximumSize(1000000)
			.build();
	private static Map<String, Map<String,String>> zeroCache = new java.util.concurrent.ConcurrentHashMap<String, Map<String,String>>();
	private static Map<String, String[]>carInfoArray=new HashMap<String, String[]>();
	static{
		setTime();
		redis=new DataToRedis();
		Map<String, String> map = redis.hgetallMapByKeyAndDb("XNY.CARINFO", 0);
		carInfoCache.putAll(map);
		Map<String,Set<String>> carUsers = RedisOrganizationUtil.getCarUser(false);
		carUserCache.putAll(carUsers);
		for (Map.Entry<String, String> entry : map.entrySet()) {
			try {
				String key = entry.getKey();
				String value = entry.getValue();
				
				if (ObjectUtils.isNullOrEmpty(key)
						|| ObjectUtils.isNullOrEmpty(value)) 
					continue;

				String []strings=value.split(",",-1);
				
				if(strings.length != 15)
					continue;
				carInfoArray.put(key, strings);
			}catch (Exception e) {
					
			}
		}
		tasks();
	}
	private static void resetAllCache(){
		if (null != carInfoCache) 
			carInfoCache.cleanUp();
		Map<String, String> map = redis.hgetallMapByKeyAndDb("XNY.CARINFO", 0);
		carInfoCache.putAll(map);
		if (null != carInfoArray) 
			carInfoArray.clear();
		for (Map.Entry<String, String> entry : map.entrySet()) {
			try {
				String key = entry.getKey();
				String value = entry.getValue();
				
				if (ObjectUtils.isNullOrEmpty(key)
						|| ObjectUtils.isNullOrEmpty(value)) 
					continue;

				String []strings=value.split(",",-1);
				
				if(strings.length != 15)
					continue;
				carInfoArray.put(key, strings);
			}catch (Exception e) {
					
			}
		}
		resetUserCache();
	}
	private static void setTime(){
		String offli=ConfigUtils.sysDefine.getProperty(StormConfigKey.REDIS_OFFLINE_SECOND);
		if(null != offli)
			time=1000*Long.valueOf(offli);
		String stopli=ConfigUtils.sysDefine.getProperty("redis.offline.stoptime");
		if(null != stopli)
			stoptime=1000*Long.valueOf(stopli);
	}
	private static void tasks(){
		long clustertime = 180L;
		long stattime = 600L;
		try {
			String cltt = ConfigUtils.sysDefine.getProperty("redis.totalInterval");
			String stt = ConfigUtils.sysDefine.getProperty("redis.monitor.time");
			if (null != cltt) 
				clustertime=Long.valueOf(cltt);
			if (null != stt) 
				stattime=Long.valueOf(stt);
			
		} catch (Exception e) {
			
		}
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new TimerThread(), 0, clustertime, TimeUnit.SECONDS);
//		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new CacheThread(), 30, 2*Long.parseLong(ConfigUtils.sysDefine.getProperty("redis.totalInterval")), TimeUnit.SECONDS);
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new StatCarinfoThread(), 0, stattime, TimeUnit.SECONDS);
	}

	static class CacheThread implements Runnable{

		@Override
		public void run() {
			
		}
	}

	public static String carInfoByVin(final String vin){
		String carInfo=unknowString;
		try {
			carInfo=carInfoCache.get(vin, new Callable<String>() {

				@Override
				public String call() throws Exception {
					String string=null;
					try {
						int i = 0;
						while (i < 3) {
							i++;
							string=redis.hgetBykeyAndFiled("XNY.CARINFO", vin, 0);
							if(!ObjectUtils.isNullOrEmpty(string))
								return string;
							TimeUnit.MILLISECONDS.sleep(3);
						}
					} catch (Exception e) {
						e.printStackTrace();
						TimeUnit.MILLISECONDS.sleep(3);
						string=redis.hgetBykeyAndFiled("XNY.CARINFO", vin, 0);
						if(!ObjectUtils.isNullOrEmpty(string))
							return string;
					}
					return unknowString;
				}
			});
		} catch (Exception e) {
			try {
				TimeUnit.MILLISECONDS.sleep(200);
				carInfo=carInfoByVin(vin);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		return carInfo;
	}

	private static void resetUserCache(){
		try {
			boolean bool = false;
			Set<String>sets=redis.getKeysSet(10, "XNY.INFOCHANG.*");
			if(null!=sets && 0<sets.size()){

				for (String str : sets) {
					bool="1".equals(redis.getString(10, str));
					if (bool) 
						break;
				}
				if (bool) {
					for (String str : sets) {
						redis.setString(10, str, "0");
					}
					carUserCache.cleanUp();
					Map<String,Set<String>> carUsers = RedisOrganizationUtil.getCarUser(bool);
					carUserCache.putAll(carUsers);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static Set<String> carUserByVin(final String vin){
		Set<String> users=null;
		
		final boolean change=false;
		try {
			users=carUserCache.get(vin, new Callable<Set<String>>() {

				@Override
				public Set<String> call() throws Exception {
					Map<String, Set<String>> map=RedisOrganizationUtil.getCarUser(change);
					if(null==map)
						return null;
					Set<String> set=map.get(vin);
					return set;
				}
			});
		} catch (Exception e) {
//			e.printStackTrace();
		}
		return users;
	}

	public static Set<String> parentDistrictByVin(String districtId){
		try {
			return RedisOrganizationUtil.getDistrictClassify().get(districtId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void init(){
		RedisOrganizationUtil.init();
	}

	static class TimerThread implements Runnable{
		private Map<String, Map<String,String>>infoMap;
		private Map<String, Map<String,String>>infoNoDistMap;
		private Map<String, Map<String,String>>infoTotalMap;
		private Map<String, Map<String,String>>infoTotalDisMap;
		private Map<String, Map<String,String>>districtTotalInfo;
		private Map<String, Map<String,String>>districtTotalTypeInfo;
		private Map<String, Map<String,String>>orgDistAndTypeMap;
		private Map<String, Map<String,String>>orgOnlyDistMap;
		private Map<String, Map<String,String>>orgOnlyCarTypeMap;
		private Map<String, Map<String,String>>plantOnlyCarTypeMap;
		
		private Map<String, Map<String, Map<String,String>>>userCacheDistAndTypeMap;
		private Map<String, Map<String, Map<String,String>>>userCacheOnlyDistMap;
		private Map<String, Map<String, Map<String,String>>>userCacheOnlyCarTypeMap;
		private Map<String, Map<String, Map<String,String>>>userTotalDistAndTypeMap;
		private Map<String, Map<String, Map<String,String>>>userTotalOnlyDistMap;
		private static boolean isclear=false;
		private static boolean keyStaComp =false;
		private static long stime =0L;
		private ExecutorService threadPool;
		{
			infoMap=new ConcurrentHashMap<String,Map<String, String>>();
			infoNoDistMap=new ConcurrentHashMap<String,Map<String, String>>();
			infoTotalMap=new ConcurrentHashMap<String,Map<String, String>>();
			infoTotalDisMap=new ConcurrentHashMap<String,Map<String, String>>();
			districtTotalInfo=new ConcurrentHashMap<String,Map<String, String>>();
			districtTotalTypeInfo=new ConcurrentHashMap<String,Map<String, String>>();
			
			orgDistAndTypeMap=new ConcurrentHashMap<String,Map<String, String>>();
			orgOnlyDistMap=new ConcurrentHashMap<String,Map<String, String>>();
			orgOnlyCarTypeMap=new ConcurrentHashMap<String,Map<String, String>>();
			plantOnlyCarTypeMap=new ConcurrentHashMap<String,Map<String, String>>();
			
			userCacheDistAndTypeMap=new ConcurrentHashMap<String, Map<String, Map<String,String>>>();
			userCacheOnlyDistMap=new ConcurrentHashMap<String, Map<String, Map<String,String>>>();
			userCacheOnlyCarTypeMap=new ConcurrentHashMap<String, Map<String, Map<String,String>>>();
			userTotalDistAndTypeMap=new HashMap<String, Map<String, Map<String,String>>>();
			userTotalOnlyDistMap=new HashMap<String, Map<String, Map<String,String>>>();
			threadPool=Executors.newCachedThreadPool();
			
			stime = 1506790800000L;//2017/10/01/01:00:00的long值时间
		}
		@Override
        public void run() {
            try {
            	int db=9;
//            	long now = System.currentTimeMillis();
//            	if ((now -stime)%86400000<=180000) {
//            		redis.flushDB(db);
//				}
            	
            	if (!isclear) {
					
            		initVehicle();
            		
            		if (keyStaComp) {
						
//            			delKeys(db);//太耗费资源，每次插入都要删除
            			saveMap(infoMap,db);
            			
            			saveMap(infoNoDistMap,db);
            			saveMap(infoTotalMap,db);
            			saveMap(infoTotalDisMap,db);
            			saveMap(districtTotalInfo,db);
            			saveMap(districtTotalTypeInfo,db);
            			
            			Collection<Map<String, Map<String, String>>> collection=null;
            			collection=userCacheDistAndTypeMap.values();
            			saveCollection(collection,db);
            			
            			collection=userCacheOnlyDistMap.values();
            			saveCollection(collection,db);
            			
            			collection=userCacheOnlyCarTypeMap.values();
            			saveCollection(collection,db);
            			
            			collection=userTotalDistAndTypeMap.values();
            			saveCollection(collection,db);
            			
            			collection=userTotalOnlyDistMap.values();
            			saveCollection(collection,db);
            			collection=null;
            			
            			saveMap(orgDistAndTypeMap,db);
            			saveMap(orgOnlyDistMap,db);
            			saveMap(orgOnlyCarTypeMap,db);
            			saveMap(plantOnlyCarTypeMap,db);
            			
            			delGCKeys(db);
            			keyStaComp =false;
            			isclear=false;
					}
				}
            	
            } catch (Exception e) {
                System.out.println("------临时统计任务异常！" + e);
                e.printStackTrace();
            }
        }

		private void delKeys(int db){
			try {
				Set<String>keys=redis.getKeysSet(db, "CARINFO.*");
				redis.delKeys(keys, db);
				keys=redis.getKeysSet(db, "ORGCAR.*");
				redis.delKeys(keys, db);
				keys=redis.getKeysSet(db, "PLANTCAR.*");
				redis.delKeys(keys, db);
				keys=redis.getKeysSet(db, "USERCAR.*");
				redis.delKeys(keys, db);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		private void delGCKeys(int db){
			try {
				Set<String>keys = getGCKeys();
				redis.delKeys(keys, db);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		private Set<String> getGCKeys(){
			Set<String> gc = new HashSet<String>();
			Set<String>localkeys = memoryKeys();
			Set<String>exsistkeys = dbexsistKeys(9);
			for (String string : exsistkeys) {
				if (! localkeys.contains(string.trim())) {
					gc.add(string);
				}
			}
			return gc;
		}
		private Set<String> memoryKeys(){
			Set<String>localkeys = new HashSet<String>();
			addKeys(localkeys,infoMap);
			addKeys(localkeys,infoNoDistMap);
			addKeys(localkeys,infoTotalMap);
			addKeys(localkeys,infoTotalDisMap);
			addKeys(localkeys,districtTotalInfo);
			addKeys(localkeys,districtTotalTypeInfo);
			addKeys(localkeys,orgDistAndTypeMap);
			addKeys(localkeys,orgOnlyDistMap);
			addKeys(localkeys,orgOnlyCarTypeMap);
			addKeys(localkeys,plantOnlyCarTypeMap);
			
			addKeys(localkeys,userCacheDistAndTypeMap.values());
			addKeys(localkeys,userCacheOnlyDistMap.values());
			addKeys(localkeys,userCacheOnlyCarTypeMap.values());
			addKeys(localkeys,userTotalDistAndTypeMap.values());
			addKeys(localkeys,userTotalOnlyDistMap.values());
			
			return localkeys;
		}
		
		private void addKeys(Set<String> set,Map<String, Map<String,String>> map){
			if (null != map && map.size() > 0) 
				set.addAll(map.keySet());
		}
		
		private void addKeys(Set<String> set,Collection<Map<String, Map<String,String>>> maps){
			if (null != maps && maps.size() > 0) {
				for (Map<String, Map<String, String>> map : maps) {
					addKeys(set,map);
				}
			}
		}
		
		private Set<String> dbexsistKeys(int db){
			Set<String>exsistkeys = new HashSet<String>();
			Set<String>keys=redis.getKeysSet(db, "CARINFO.*");
			if(null != keys && keys.size()>0)
				exsistkeys.addAll(keys);
			keys=redis.getKeysSet(db, "ORGCAR.*");
			if(null != keys && keys.size()>0)
				exsistkeys.addAll(keys);
			keys=redis.getKeysSet(db, "PLANTCAR.*");
			if(null != keys && keys.size()>0)
				exsistkeys.addAll(keys);
			keys=redis.getKeysSet(db, "USERCAR.*");
			if(null != keys && keys.size()>0)
				exsistkeys.addAll(keys);
			return exsistkeys;
		}
		private void saveCollection(Collection<Map<String, Map<String, String>>> collection,int db){
			try {
				for (Map<String, Map<String, String>> map : collection) {
					saveMap(map,db);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		private void saveMap(Map<String, Map<String, String>> map,int db){
			try {
				if(null!=map)
					for(Map.Entry<String, Map<String,String>> entry:map.entrySet()){
						String key = entry.getKey();
						Map<String,String> value = entry.getValue();
						if (ObjectUtils.isNullOrEmpty(key) 
								|| ObjectUtils.isNullOrEmpty(value) ) 
							continue;

						redis.saveMap(value, db, key);
					}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
        private void initVehicle() throws Exception {
            CTFOCacheKeys ctfoCacheKeys=null;
            try {
            	isclear=true;
            	keyStaComp=false;
                ctfoCacheKeys = CTFOUtils.getDefaultCTFOCacheTable().getCTFOCacheKeys();
                List<OneKeysExecutor> executors=new LinkedList<OneKeysExecutor>();
                while(ctfoCacheKeys.next()){
                	List<String>keys=ctfoCacheKeys.getKeys();
                	executors.add(new OneKeysExecutor(keys));
                }
                for (OneKeysExecutor executor : executors) {
					threadPool.execute(executor);
				}
                boolean exe=true;
                while(exe){
                	boolean allComplete=true;
                	for (OneKeysExecutor executor : executors) {
    					if(!executor.isComplete()){
    						allComplete=false;
    						TimeUnit.MILLISECONDS.sleep(100);
    						break;
    					}
    				}
                	if (allComplete) 
                		exe=false;
                }
                clearMap();
                for (OneKeysExecutor executor : executors) {
                	statistics(executor.infoMap,infoMap);
                	statistics(executor.infoNoDistMap,infoNoDistMap);
                	statistics(executor.infoTotalMap,infoTotalMap);
                	statistics(executor.infoTotalDisMap,infoTotalDisMap);
                	statistics(executor.districtTotalInfo,districtTotalInfo);
                	statistics(executor.districtTotalTypeInfo,districtTotalTypeInfo);
                	statistics(executor.orgDistAndTypeMap,orgDistAndTypeMap);
                	statistics(executor.orgOnlyDistMap,orgOnlyDistMap);
                	statistics(executor.orgOnlyCarTypeMap,orgOnlyCarTypeMap);
                	statistics(executor.plantOnlyCarTypeMap,plantOnlyCarTypeMap);

                	statisticsUser(executor.userCacheDistAndTypeMap,userCacheDistAndTypeMap);
                	statisticsUser(executor.userCacheOnlyDistMap,userCacheOnlyDistMap);
                	statisticsUser(executor.userCacheOnlyCarTypeMap,userCacheOnlyCarTypeMap);
                	statisticsUser(executor.userTotalDistAndTypeMap,userTotalDistAndTypeMap);
                	statisticsUser(executor.userTotalOnlyDistMap,userTotalOnlyDistMap);
				}
                keyStaComp=true;
                ctfoCacheKeys=null;
                executors=null;
            } catch (Exception e) {
                System.out.println("--------redis初始化实时数据计算异常！" + e);
                e.printStackTrace();
                throw e;
            }

        }

        private synchronized void statisticsUser(Map<String, Map<String, Map<String,String>>>from,Map<String, Map<String, Map<String,String>>>total){
        	if (ObjectUtils.isNullOrEmpty(from)) 
				return;
        	for (Map.Entry<String, Map<String, Map<String,String>>> entry : from.entrySet()){
        		try {
					String user=entry.getKey();
					Map<String, Map<String,String>> fromMap=entry.getValue();
					if(ObjectUtils.isNullOrEmpty(user) || ObjectUtils.isNullOrEmpty(fromMap))
						continue;
					Map<String, Map<String, String>>totalMap=total.get(user);
					if (null==totalMap) 
						totalMap=new HashMap<String,Map<String, String>>();
					statistics(fromMap, totalMap);
					total.put(user, totalMap);
				} catch (Exception e) {
					e.printStackTrace();
				}
        	}
        }

        private synchronized void statistics(Map<String, Map<String, String>>fromMap,Map<String, Map<String, String>>totalMap){

        	if (ObjectUtils.isNullOrEmpty(fromMap)) 
				return;
			
        	for (Map.Entry<String, Map<String, String>> entry : fromMap.entrySet()) {
				try {
					String key=entry.getKey();
					Map<String, String> data=entry.getValue();
					if(ObjectUtils.isNullOrEmpty(key) || ObjectUtils.isNullOrEmpty(data))
						continue;

					Map<String, String>dataMap=totalMap.get(key);
					if(null==dataMap)
						dataMap=new TreeMap<String, String>();
					double totalmileage=null==dataMap.get(SysDefine.MILEAGE_TOTAL)?0:Double.valueOf(dataMap.get(SysDefine.MILEAGE_TOTAL));
					long totalonline=null==dataMap.get(SysDefine.ONLINE_COUNT)?0:Long.valueOf(dataMap.get(SysDefine.ONLINE_COUNT));
					long totalcaractive=null==dataMap.get(SysDefine.CAR_ACTIVE_COUNT)?0:Long.valueOf(dataMap.get(SysDefine.CAR_ACTIVE_COUNT));
					long totalfault=null==dataMap.get(SysDefine.FAULT_COUNT)?0:Long.valueOf(dataMap.get(SysDefine.FAULT_COUNT));
					long totalcartotal=null==dataMap.get(SysDefine.CAR_COUNT)?0:Long.valueOf(dataMap.get(SysDefine.CAR_COUNT));
					long totalcharge=null==dataMap.get(SysDefine.CHARGE_CAR_COUNT)?0:Long.valueOf(dataMap.get(SysDefine.CHARGE_CAR_COUNT));
					long totalrunning=null==dataMap.get(SysDefine.RUNNING_ONLINE)?0:Long.valueOf(dataMap.get(SysDefine.RUNNING_ONLINE));
					long totalstop=null==dataMap.get(SysDefine.STOP_ONLINE)?0:Long.valueOf(dataMap.get(SysDefine.STOP_ONLINE));
					
					double mileage=null==data.get(SysDefine.MILEAGE_TOTAL)?0:Double.valueOf(data.get(SysDefine.MILEAGE_TOTAL));
					long online=null==data.get(SysDefine.ONLINE_COUNT)?0:Long.valueOf(data.get(SysDefine.ONLINE_COUNT));
					long carcative=null==data.get(SysDefine.CAR_ACTIVE_COUNT)?0:Long.valueOf(data.get(SysDefine.CAR_ACTIVE_COUNT));
					long fault=null==data.get(SysDefine.FAULT_COUNT)?0:Long.valueOf(data.get(SysDefine.FAULT_COUNT));
					long cartotal=null==data.get(SysDefine.CAR_COUNT)?0:Long.valueOf(data.get(SysDefine.CAR_COUNT));
					long charge=null==data.get(SysDefine.CHARGE_CAR_COUNT)?0:Long.valueOf(data.get(SysDefine.CHARGE_CAR_COUNT));
					long runningonline=null==data.get(SysDefine.RUNNING_ONLINE)?0:Long.valueOf(data.get(SysDefine.RUNNING_ONLINE));
					long stoponline=null==data.get(SysDefine.STOP_ONLINE)?0:Long.valueOf(data.get(SysDefine.STOP_ONLINE));
					
					totalmileage+=mileage;
					totalonline+=online;
					totalcaractive+=carcative;
					totalfault+=fault;
					totalcartotal+=cartotal;
					totalcharge+=charge;
					totalrunning+=runningonline;
					totalstop+=stoponline;
					
					dataMap.put(SysDefine.MILEAGE_TOTAL,""+totalmileage);
					dataMap.put(SysDefine.ONLINE_COUNT,""+totalonline);
					dataMap.put(SysDefine.CAR_ACTIVE_COUNT,""+totalcaractive);
					dataMap.put(SysDefine.FAULT_COUNT,""+totalfault);
					dataMap.put(SysDefine.CAR_COUNT,""+totalcartotal);
					dataMap.put(SysDefine.CHARGE_CAR_COUNT,""+totalcharge);
					dataMap.put(SysDefine.RUNNING_ONLINE,""+totalrunning);
					dataMap.put(SysDefine.STOP_ONLINE,""+totalstop);
					
					totalMap.put(key, dataMap);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
        }
        void clearMap(){
        	if(infoMap.size()>0){
        		infoMap.clear();
        		infoNoDistMap.clear();
        		infoTotalMap.clear();
        		infoTotalDisMap.clear();
        		districtTotalInfo.clear();
        		districtTotalTypeInfo.clear();
        		orgDistAndTypeMap.clear();
    			orgOnlyDistMap.clear();
    			orgOnlyCarTypeMap.clear();
    			plantOnlyCarTypeMap.clear();
    			
    			Collection<Map<String, Map<String, String>>> collection=null;
    			
    			collection=userCacheDistAndTypeMap.values();
    			clearCollection(collection);
    			
    			collection=userCacheOnlyDistMap.values();
    			clearCollection(collection);
    			
    			collection=userCacheOnlyCarTypeMap.values();
    			clearCollection(collection);
    			
    			collection=userTotalDistAndTypeMap.values();
    			clearCollection(collection);
    			
    			collection=userTotalOnlyDistMap.values();
    			clearCollection(collection);
    			
    			collection=null;
    			isclear=true;
        	}
        }
        void clearCollection(Collection<Map<String, Map<String, String>>> collection){
        	for (Map<String, Map<String, String>> map : collection) {
				if(null!=map)
					map.clear();
			}
        }
        
    }

	static class OneKeysExecutor implements Runnable{
		private List<String>keys;
		private Map<String, Map<String,String>>infoMap;
		private Map<String, Map<String,String>>infoNoDistMap;
		private Map<String, Map<String,String>>infoTotalMap;
		private Map<String, Map<String,String>>infoTotalDisMap;
		private Map<String, Map<String,String>>districtTotalInfo;
		private Map<String, Map<String,String>>districtTotalTypeInfo;
		private Map<String, Map<String,String>>orgDistAndTypeMap;
		private Map<String, Map<String,String>>orgOnlyDistMap;
		private Map<String, Map<String,String>>orgOnlyCarTypeMap;
		private Map<String, Map<String,String>>plantOnlyCarTypeMap;
		
		private Map<String, Map<String, Map<String,String>>>userCacheDistAndTypeMap;
		private Map<String, Map<String, Map<String,String>>>userCacheOnlyDistMap;
		private Map<String, Map<String, Map<String,String>>>userCacheOnlyCarTypeMap;
		private Map<String, Map<String, Map<String,String>>>userTotalDistAndTypeMap;
		private Map<String, Map<String, Map<String,String>>>userTotalOnlyDistMap;
		private Map<String, Boolean>vinMap;
		private boolean complete;
		{
			infoMap=new HashMap<String,Map<String, String>>();
			infoNoDistMap=new HashMap<String,Map<String, String>>();
			infoTotalMap=new HashMap<String,Map<String, String>>();
			infoTotalDisMap=new HashMap<String,Map<String, String>>();
			districtTotalInfo=new HashMap<String,Map<String, String>>();
			districtTotalTypeInfo=new HashMap<String,Map<String, String>>();
			
			orgDistAndTypeMap=new HashMap<String,Map<String, String>>();
			orgOnlyDistMap=new HashMap<String,Map<String, String>>();
			orgOnlyCarTypeMap=new HashMap<String,Map<String, String>>();
			plantOnlyCarTypeMap=new HashMap<String,Map<String, String>>();
			
			userCacheDistAndTypeMap=new HashMap<String, Map<String, Map<String,String>>>();
			userCacheOnlyDistMap=new HashMap<String, Map<String, Map<String,String>>>();
			userCacheOnlyCarTypeMap=new HashMap<String, Map<String, Map<String,String>>>();
			userTotalDistAndTypeMap=new HashMap<String, Map<String, Map<String,String>>>();
			userTotalOnlyDistMap=new HashMap<String, Map<String, Map<String,String>>>();
			
			vinMap=new HashMap<String,Boolean>();
			setComplete(false);
		}
		public OneKeysExecutor(List<String> keys) {
			super();
			this.keys = keys;
		}

		@Override
		public void run() {

			if(null!=keys && keys.size()>0)
				executeBykeys(keys);
			setComplete(true);
		}

		void executeBykeys(List<String> keys){
			try {
				for(String key :keys){
					if (ObjectUtils.isNullOrEmpty(key)) 
						continue;
					key=key.split("-",3)[2];
					if (ObjectUtils.isNullOrEmpty(key)) 
						continue;
					Map<String, String> map=CTFOUtils.getDefaultCTFOCacheTable().queryHash(key);
					if(ObjectUtils.isNullOrEmpty(map))
						continue;
					String vin=map.get("VIN");
					if(vinMap.containsKey(vin))
						continue;
					vinMap.put(vin, true);
					if (!ObjectUtils.isNullOrEmpty(vin)) {
        				String []strings=carInfoArray.get(vin);
						if(null == strings || strings.length != 15){
							logger.info("carInfo vin:"+vin);
							logger.info("carInfo vid is gc:"+map.get(DataKey.VEHICLE_ID));
							continue;
						}
						if (null == strings[0] || !strings[0].equals(map.get(DataKey.VEHICLE_ID))) {
							logger.info("vin map not exist:"+vin);
							logger.info("---- vid is not exist:"+map.get(DataKey.VEHICLE_ID));
							continue;
						}
						long now = System.currentTimeMillis();
						String org=strings[3];
						String plant=strings[5];
						String district=strings[9];
						String carType=strings[10];
            			String keyDistAndType="CARINFO."+district+"."+carType;
            			execu(now,map,keyDistAndType,infoMap);
            			String keyOnlyCarType="CARINFO.TOTAL."+district;
            			execu(now,map,keyOnlyCarType,infoTotalMap);
            			String keyOnlyDist="CARINFO.DISTOTAL."+carType;
            			execu(now,map,keyOnlyDist,infoTotalDisMap);

            			Set<String> keyDistricts=parentDistrictByVin(district);
            			if(null!=keyDistricts && 0<keyDistricts.size()){
            				for (String dis : keyDistricts) {
            					String keyDist="CARINFO.DISTRICTS."+dis;
                    			execu(now,map,keyDist,districtTotalInfo);
                    			
                    			String keyDistType="CARINFO.DISTRICTS."+dis+"."+carType;
                    			execu(now,map,keyDistType,districtTotalTypeInfo);
							}
            			}

            			if(unknow.equals(district)){
            				String keyNoUnk="CARINFO.NOUNKNOW."+carType;
                			execu(now,map,keyNoUnk,infoNoDistMap);
            			}

            			org=ObjectUtils.isNullOrEmpty(org)?unknow:org;
            			String orgDistAndType="ORGCAR."+org+"."+district+"."+carType;
            			execu(now,map,orgDistAndType,orgDistAndTypeMap);
            			
            			String orgOnlyDist="ORGCAR."+org+".DIST."+district;
            			execu(now,map,orgOnlyDist,orgOnlyDistMap);
            			
            			String orgOnlyCarType="ORGCAR."+org+".CARTYPE."+carType;
            			execu(now,map,orgOnlyCarType,orgOnlyCarTypeMap);
            			
            			String plantOnlyCarType="PLANTCAR."+plant+".CARTYPE."+carType;
            			execu(now,map,plantOnlyCarType,plantOnlyCarTypeMap);

//            			String userInfo=userInfoByVin(vin);
//            			String[]users=userInfo.split(",");
            			Set<String>users=carUserByVin(vin);
            			if(null!=users)
            			for (String user : users) {
            				if (ObjectUtils.isNullOrEmpty(user)) 
								continue;
            				Map<String, Map<String,String>>userDistAndTypeMap=userCacheDistAndTypeMap.get(user);
            				if (null==userDistAndTypeMap) 
								userDistAndTypeMap=new HashMap<String,Map<String, String>>();
							
            				Map<String, Map<String,String>>userOnlyDistMap=userCacheOnlyDistMap.get(user);
            				if (null==userOnlyDistMap) 
            					userOnlyDistMap=new HashMap<String,Map<String, String>>();
            				
            				Map<String, Map<String,String>>userOnlyCarTypeMap=userCacheOnlyCarTypeMap.get(user);
            				if (null==userOnlyCarTypeMap) 
            					userOnlyCarTypeMap=new HashMap<String,Map<String, String>>();
            				
            				String userDistAndType="USERCAR."+user+"."+district+"."+carType;
                			execu(now,map,userDistAndType,userDistAndTypeMap);
                			
                			String userOnlyDist="USERCAR."+user+".DIST."+district;
                			execu(now,map,userOnlyDist,userOnlyDistMap);
                			
                			String userOnlyCarType="USERCAR."+user+".CARTYPE."+carType;
                			execu(now,map,userOnlyCarType,userOnlyCarTypeMap);

            				userCacheDistAndTypeMap.put(user, userDistAndTypeMap);
            				userCacheOnlyDistMap.put(user, userOnlyDistMap);
            				userCacheOnlyCarTypeMap.put(user, userOnlyCarTypeMap);
            				
            				if(null!=keyDistricts && 0<keyDistricts.size()){
            					
            					Map<String, Map<String,String>>userDistrictsAndTypeMap=userTotalDistAndTypeMap.get(user);
                				if (null==userDistrictsAndTypeMap) 
                					userDistrictsAndTypeMap=new HashMap<String,Map<String, String>>();
    							
                				Map<String, Map<String,String>>userOnlyDistrictsMap=userTotalOnlyDistMap.get(user);
                				if (null==userOnlyDistrictsMap) 
                					userOnlyDistrictsMap=new HashMap<String,Map<String, String>>();
                				
                				for (String dis : keyDistricts) {
                					String keyDist="USERCAR.DISTRICTS."+user+"."+dis;
                        			execu(now,map,keyDist,userDistrictsAndTypeMap);
                        			
                        			String keyDistType="USERCAR.DISTRICTS."+user+"."+dis+"."+carType;
                        			execu(now,map,keyDistType,userOnlyDistrictsMap);
    							}
                				userTotalDistAndTypeMap.put(user, userDistrictsAndTypeMap);
                				userTotalOnlyDistMap.put(user, userOnlyDistrictsMap);
                			}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		synchronized void execu(long now,Map<String, String> map,String keyty,Map<String, Map<String,String>>infoMap){
        	try {
				Map<String, String>disMap=infoMap.get(keyty);
				if(null==disMap)
					disMap=new TreeMap<String, String>();
				double mileage=null==disMap.get(SysDefine.MILEAGE_TOTAL)?0:Double.valueOf(disMap.get(SysDefine.MILEAGE_TOTAL));
				long online=null==disMap.get(SysDefine.ONLINE_COUNT)?0:Long.valueOf(disMap.get(SysDefine.ONLINE_COUNT));
				long caractive=null==disMap.get(SysDefine.CAR_ACTIVE_COUNT)?0:Long.valueOf(disMap.get(SysDefine.CAR_ACTIVE_COUNT));
				long fault=null==disMap.get(SysDefine.FAULT_COUNT)?0:Long.valueOf(disMap.get(SysDefine.FAULT_COUNT));
				long cartotal=null==disMap.get(SysDefine.CAR_COUNT)?0:Long.valueOf(disMap.get(SysDefine.CAR_COUNT));
				long charge=null==disMap.get(SysDefine.CHARGE_CAR_COUNT)?0:Long.valueOf(disMap.get(SysDefine.CHARGE_CAR_COUNT));
				long runningonline=null==disMap.get(SysDefine.RUNNING_ONLINE)?0:Long.valueOf(disMap.get(SysDefine.RUNNING_ONLINE));
				long stoponline=null==disMap.get(SysDefine.STOP_ONLINE)?0:Long.valueOf(disMap.get(SysDefine.STOP_ONLINE));
				
				mileage +=Double.valueOf(NumberUtils.stringNumber(map.get(DataKey._2202_TOTAL_MILEAGE)));
				long lastTime=Long.valueOf(map.get(SysDefine.ONLINEUTC));
				boolean istoday = timeIsToday(now, lastTime);
				if (now-lastTime<time){
					online++;
					boolean isstop=isStop(map);
					if (isstop) 
						stoponline++;
					else
						runningonline++;
				}
				if (istoday) {
					caractive++;
				}
				if("1".equals(map.get(SysDefine.ISALARM)) && null != map.get(SysDefine.ALARMUTC))
					fault++;
				if("1".equals(map.get("2301")) 
						|| "2".equals(map.get("2301"))
						|| "4".equals(map.get("2301")))
					charge++;
				cartotal++;
				disMap.put(SysDefine.MILEAGE_TOTAL,""+mileage);
				disMap.put(SysDefine.ONLINE_COUNT,""+online);
				disMap.put(SysDefine.CAR_ACTIVE_COUNT,""+caractive);
				disMap.put(SysDefine.FAULT_COUNT,""+fault);
				disMap.put(SysDefine.CAR_COUNT,""+cartotal);
				disMap.put(SysDefine.CHARGE_CAR_COUNT,""+charge);
				disMap.put(SysDefine.RUNNING_ONLINE,""+runningonline);
				disMap.put(SysDefine.STOP_ONLINE,""+stoponline);
				infoMap.put(keyty, disMap);
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
		private boolean timeIsToday(long now,long time){
			long space = now - standard;
			int days = (int)(space/86400000);
			if (standard+86400000L*days <= time && time <= standard+86400000L*(days+1)) {
				return true;
			}
			return false;
		}
		boolean isStop(Map<String, String> map){
			try {
				String vid = map.get(DataKey.VEHICLE_ID);
				String rev = map.get("2303");
				String spd = map.get("2201");
				if (!"0".equals(spd) || !"20000".equals(rev)) {
					zeroCache.remove(vid);
					return false;
				}
				if ("0".equals(spd) && "20000".equals(rev)){
					String timelong = map.get(SysDefine.ONLINEUTC);
					String lon = map.get("2502");//经度
					String lan = map.get("2503");//纬度
					
					Map<String , String>startZero=zeroCache.get(vid);
					if (null == startZero) {
						startZero = new TreeMap<String , String>();
						startZero.put("2303", rev);
						startZero.put("2201", spd);
						startZero.put("2502", lon);
						startZero.put("2503", lan);
						startZero.put(SysDefine.ONLINEUTC, timelong);
						
						zeroCache.put(vid, startZero);
						return false;
					} else {
						long lastTime=Long.valueOf(map.get(SysDefine.ONLINEUTC));
						long starttime=Long.valueOf(startZero.get(SysDefine.ONLINEUTC));
						if (lastTime - starttime >= stoptime) {
							String slon = startZero.get("2502");//经度
							String slan = startZero.get("2503");//纬度
							if ( ( ObjectUtils.isNullOrEmpty(lon) 
										|| ObjectUtils.isNullOrEmpty(lan) )
									&& ( ObjectUtils.isNullOrEmpty(slon)
											|| ObjectUtils.isNullOrEmpty(slan) ) ) 
								return true;
							if (   ! ObjectUtils.isNullOrEmpty(lon) 
								&& ! ObjectUtils.isNullOrEmpty(lan) 
								&& ! ObjectUtils.isNullOrEmpty(slon)
								&& ! ObjectUtils.isNullOrEmpty(slan) ){
								long longi = Long.valueOf(lon);
								long slongi = Long.valueOf(slon);
								long lati = Long.valueOf(lan);
								long slati = Long.valueOf(slan);
								if (Math.abs(longi-slongi)<=2
										&& Math.abs(lati-slati)<=2) 
									return true;
							}
								
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return false;
		}
		public boolean isComplete() {
			return complete;
		}

		public void setComplete(boolean complete) {
			this.complete = complete;
		}
	}

	static class StatCarinfoThread implements Runnable{

		private Map<String, Map<String, String>> stadistrictsTotalMap;
		private Map<String, Map<String, String>> stadistrictsTotalTypeMap;
		private Map<String, Map<String, String>> staplantOnlyCarTypeMap;
		private Map<String,Map<String, Map<String, String>>> stauserTotalDistAndTypeMap;
		private Map<String,Map<String, Map<String, String>>> stauserTotalOnlyDistMap;
		private static boolean staComp =false;
		private static boolean isclearSta =false;
		private static int poolsz = 120;
		private ExecutorService threadPools;
		{
			stadistrictsTotalMap = new ConcurrentHashMap<String, Map<String, String>>();
			stadistrictsTotalTypeMap = new ConcurrentHashMap<String, Map<String, String>>();
			staplantOnlyCarTypeMap = new ConcurrentHashMap<String, Map<String, String>>();
			stauserTotalDistAndTypeMap = new ConcurrentHashMap<String,Map<String, Map<String, String>>>();
			stauserTotalOnlyDistMap = new ConcurrentHashMap<String,Map<String, Map<String, String>>>();
			String threads=ConfigUtils.sysDefine.getProperty("stat.thread.no");
			if(null != threads)
				poolsz=Integer.valueOf(threads);
			threadPools = Executors.newFixedThreadPool(poolsz);
		}

        
		@Override
		public void run() {
			if (!isclearSta) {
        		try {
					statistic(carInfoArray);
					if (staComp) {
						
						saveOrUpdate(9);
						delGCKeys(9);
						staComp=false;
						resetAllCache();
						isclearSta=false;
					}
				} catch (Exception e) {
					System.err.println("XNY.CARINFO 统计异常："+e);
					e.printStackTrace();
				}
			}

		}
		

/*******************************************************/
        
		void saveOrUpdate(int db){
    		saveMap(stadistrictsTotalMap,db);
    		saveMap(stadistrictsTotalTypeMap,db);
    		
    		Collection<Map<String, Map<String, String>>> collection=null;
    		collection=stauserTotalDistAndTypeMap.values();
    		saveCollection(collection,db);
    		
    		collection=stauserTotalOnlyDistMap.values();
    		saveCollection(collection,db);
    		collection=null;
    		
		}
		
		void statistic(Map<String, String[]> map) throws Exception{
			if (isclearSta) 
				return;
			if (null == map || map.size() ==0) 
				return;

			System.out.println("---map size:"+map.size());
			clearStaMap();
			staComp=false;
			for (Map.Entry<String, String[]> entry : map.entrySet()) {
				try {
					String key = entry.getKey();
					String[] strings = entry.getValue();
					
					if (ObjectUtils.isNullOrEmpty(key)
							|| ObjectUtils.isNullOrEmpty(strings)) 
						continue;

					if(strings.length != 15)
						continue;

					threadPools.execute(new UserDistrictExecutor(key,strings));
					//String org=strings[3];
					/*String district=strings[9];
					String carType=strings[10];
					String monitor=strings[14];
					boolean ismonitor="1".equals(monitor);
					String plant=strings[5];
					if (!ObjectUtils.isNullOrEmpty(plant) && !ObjectUtils.isNullOrEmpty(carType)) {
						
						String plantOnlyCarType="STATISTIC.PLANTCAR."+plant+".CARTYPE."+carType;
						exeCalculate(plantOnlyCarType,staplantOnlyCarTypeMap,ismonitor);
					}
					
					if (ObjectUtils.isNullOrEmpty(district)) 
						continue;
					
					Set<String> districts=parentDistrictByVin(district);
					
					if (null != districts && districts.size()>0) {
						for (String dis : districts) {
							String keyDist="STATISTIC.DISTRICTS."+dis;
							exeCalculate(keyDist,stadistrictsTotalMap,ismonitor);
							
							String keyDistType="STATISTIC.DISTRICTS."+dis+"."+carType;
							exeCalculate(keyDistType,stadistrictsTotalTypeMap,ismonitor);
						}
					}
					
					Set<String>users=carUserByVin(key);
					if(null!=users)
						for (String user : users) {
							if (ObjectUtils.isNullOrEmpty(user)) 
								continue;
							
							if(null!=districts && 0<districts.size()){
								
								Map<String, Map<String,String>>userDistrictsAndTypeMap=stauserTotalDistAndTypeMap.get(user);
								if (null==userDistrictsAndTypeMap) 
									userDistrictsAndTypeMap=new HashMap<String,Map<String, String>>();
								
								Map<String, Map<String,String>>userOnlyDistrictsMap=stauserTotalOnlyDistMap.get(user);
								if (null==userOnlyDistrictsMap) 
									userOnlyDistrictsMap=new HashMap<String,Map<String, String>>();
								
								for (String dis : districts) {
									String keyDist="STATISTIC.USER.DISTRICTS."+user+"."+dis;
									exeCalculate(keyDist,userDistrictsAndTypeMap,ismonitor);
					    			
					    			String keyDistType="STATISTIC.USER.DISTRICTS."+user+"."+dis+"."+carType;
					    			exeCalculate(keyDistType,userOnlyDistrictsMap,ismonitor);
								}
								stauserTotalDistAndTypeMap.put(user, userDistrictsAndTypeMap);
								stauserTotalOnlyDistMap.put(user, userOnlyDistrictsMap);
							}
						}*/
				} catch (Exception e) {
					e.printStackTrace();
					throw e;
				}
			}
			try {
				threadPools.shutdown();
				while(true){
					if (threadPools.isTerminated())
						break;
					TimeUnit.MILLISECONDS.sleep(500);
				}
				threadPools=null;
				threadPools = Executors.newFixedThreadPool(poolsz); 
				staComp = true;
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
		}

		class UserDistrictExecutor implements Runnable{
			private String key;
			private String[]strings;
			
			public UserDistrictExecutor(String key, String[] strings) {
				super();
				this.key = key;
				this.strings = strings;
			}

			@Override
			public void run() {
				executeCal(key,strings);
			}
			
			void executeCal(String key,String[]strings){
				if (null == key || null == strings) 
					return;
				try {
					String district=strings[9];
					String carType=strings[10];
					String monitor=strings[14];
					boolean ismonitor="1".equals(monitor);
					String plant=strings[5];
					if (!ObjectUtils.isNullOrEmpty(plant) && !ObjectUtils.isNullOrEmpty(carType)) {
						
						String plantOnlyCarType="STATISTIC.PLANTCAR."+plant+".CARTYPE."+carType;
						exeCalculate(plantOnlyCarType,staplantOnlyCarTypeMap,ismonitor);
					}
					
					if (!ObjectUtils.isNullOrEmpty(district)){
						
						Set<String> districts=parentDistrictByVin(district);
						
						if (null != districts && districts.size()>0) {
							for (String dis : districts) {
								String keyDist="STATISTIC.DISTRICTS."+dis;
								exeCalculate(keyDist,stadistrictsTotalMap,ismonitor);
								
								String keyDistType="STATISTIC.DISTRICTS."+dis+"."+carType;
								exeCalculate(keyDistType,stadistrictsTotalTypeMap,ismonitor);
							}
						}
						
						Set<String>users=carUserByVin(key);
						if(null!=users)
							for (String user : users) {
								if (ObjectUtils.isNullOrEmpty(user)) 
									continue;
								
								if(null!=districts && 0<districts.size()){
									
									Map<String, Map<String,String>>userDistrictsAndTypeMap=stauserTotalDistAndTypeMap.get(user);
									if (null==userDistrictsAndTypeMap) 
										userDistrictsAndTypeMap=new HashMap<String,Map<String, String>>();
									
									Map<String, Map<String,String>>userOnlyDistrictsMap=stauserTotalOnlyDistMap.get(user);
									if (null==userOnlyDistrictsMap) 
										userOnlyDistrictsMap=new HashMap<String,Map<String, String>>();
									
									for (String dis : districts) {
										String keyDist="STATISTIC.USER.DISTRICTS."+user+"."+dis;
										exeCalculate(keyDist,userDistrictsAndTypeMap,ismonitor);
										
										String keyDistType="STATISTIC.USER.DISTRICTS."+user+"."+dis+"."+carType;
										exeCalculate(keyDistType,userOnlyDistrictsMap,ismonitor);
									}
									stauserTotalDistAndTypeMap.put(user, userDistrictsAndTypeMap);
									stauserTotalOnlyDistMap.put(user, userOnlyDistrictsMap);
								}
							}
					} 
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		
		synchronized void exeCalculate(String key,Map<String, Map<String,String>>infoMap,boolean ismonitor){
	    	try {
				Map<String, String>disMap=infoMap.get(key);
				if(null==disMap)
					disMap=new TreeMap<String, String>();
				long monitorcount=null==disMap.get(SysDefine.MONITOR_CAR_TOTAL)?0:Long.valueOf(disMap.get(SysDefine.MONITOR_CAR_TOTAL));
				long cartotal=null==disMap.get(SysDefine.CAR_TOTAL)?0:Long.valueOf(disMap.get(SysDefine.CAR_TOTAL));

				cartotal++;
				if (ismonitor) 
					monitorcount++;

				disMap.put(SysDefine.MONITOR_CAR_TOTAL,""+monitorcount);
				disMap.put(SysDefine.CAR_TOTAL,""+cartotal);
				infoMap.put(key, disMap);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }

		private void saveCollection(Collection<Map<String, Map<String, String>>> collection,int db){
			try {
				for (Map<String, Map<String, String>> map : collection) {
					saveMap(map,db);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		private void delGCKeys(int db){
			try {
				Set<String>keys = getGCKeys();
				redis.delKeys(keys, db);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		private Set<String> getGCKeys(){
			Set<String> gc = new HashSet<String>();
			Set<String>localkeys = memoryKeys();
			Set<String>exsistkeys = dbexsistStKeys(9);
			for (String string : exsistkeys) {
				if (! localkeys.contains(string)) {
					gc.add(string);
				}
			}
			return gc;
		}
		private Set<String> memoryKeys(){
			Set<String>localkeys = new HashSet<String>();
			addKeys(localkeys,stadistrictsTotalMap);
			addKeys(localkeys,stadistrictsTotalTypeMap);
			addKeys(localkeys,staplantOnlyCarTypeMap);
			
			addKeys(localkeys,stauserTotalDistAndTypeMap.values());
			addKeys(localkeys,stauserTotalOnlyDistMap.values());
			
			return localkeys;
		}
		
		private void addKeys(Set<String> set,Map<String, Map<String,String>> map){
			if (null != map && map.size() > 0) 
				set.addAll(map.keySet());
		}
		
		private void addKeys(Set<String> set,Collection<Map<String, Map<String,String>>> maps){
			if (null != maps && maps.size() > 0) {
				for (Map<String, Map<String, String>> map : maps) {
					addKeys(set,map);
				}
			}
		}
		
		private Set<String> dbexsistStKeys(int db){
			Set<String>keys=redis.getKeysSet(db, "STATISTIC.*");
			return keys;
		}
		private void saveMap(Map<String, Map<String, String>> map,int db){
			try {
				if(null!=map)
					for(Map.Entry<String, Map<String,String>> entry:map.entrySet()){
						String key = entry.getKey();
						Map<String,String> value = entry.getValue();
						if (ObjectUtils.isNullOrEmpty(key) 
								|| ObjectUtils.isNullOrEmpty(value) ) 
							continue;

						redis.saveMap(value, db, key);
					}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		synchronized void clearStaMap(){
			stadistrictsTotalMap.clear();
    		stadistrictsTotalTypeMap.clear();
    		staplantOnlyCarTypeMap.clear();
			
			Collection<Map<String, Map<String, String>>> collection=null;
			
			collection=stauserTotalDistAndTypeMap.values();
			clearCollection(collection);
			
			collection=stauserTotalOnlyDistMap.values();
			clearCollection(collection);
			
			collection=null;
			isclearSta=true;
        }

        
        /*******************************************************/
		
        void clearCollection(Collection<Map<String, Map<String, String>>> collection){
        	for (Map<String, Map<String, String>> map : collection) {
				if(null!=map)
					map.clear();
			}
        }
        
	}
	
	public static void main(String[] args) {
		RedisTotalCacheInitUtil.init();
//		String mString="82a2146d-1502-42af-bb88-e02baf0d2941,,京A954829,,,,,,954829,,,,,,0";
//		String jString="vid,终端ID,车牌号,使用单位,存放地点,汽车厂商,终端厂商, 终端类型,车辆类型,行政区域,车主,联系人,出厂时间,注册时间,是否注册";
//		
//		System.out.println(mString.split(",").length);
//		System.out.println(jString.split(",")[9]);
	}
}
