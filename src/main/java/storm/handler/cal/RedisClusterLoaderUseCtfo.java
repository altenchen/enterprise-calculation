package storm.handler.cal;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.ctfo.datacenter.cache.handle.CTFOCacheKeys;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import storm.dao.DataToRedis;
import storm.util.CTFOUtils;
import storm.util.ObjectUtils;

public class RedisClusterLoaderUseCtfo {

	private static Cache<String,Map<String,String>> carlastrecord = CacheBuilder.newBuilder()
			.expireAfterAccess(10,TimeUnit.MINUTES)
			.maximumSize(15000000)
			.build();
	private static Cache<String, String[]>carInfoCache = CacheBuilder.newBuilder()
			.expireAfterAccess(60,TimeUnit.MINUTES)
			.maximumSize(15000000)
			.build();
	private static boolean redisclusterIsload = false;
	private static boolean carinfoIsload = false;
	static LinkedBlockingQueue<String> carVids = new LinkedBlockingQueue<String>(20000000);
	
	private synchronized static void initDatByCluster(){
		redisclusterIsload = loadLastrecordByRediscluster();
	}
	
	private synchronized static void initCarinfoCache(){
		carinfoIsload = loadCarinfoCache();
	}
	
	private synchronized static boolean loadCarinfoCache(){
		try {
			DataToRedis redis=new DataToRedis();
			Map<String, String> map = redis.hgetallMapByKeyAndDb("XNY.CARINFO", 0);
			for (Map.Entry<String, String> entry : map.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				
				if (ObjectUtils.isNullOrEmpty(key)
						|| ObjectUtils.isNullOrEmpty(value)) 
					continue;
				
				String []strings=value.split(",",-1);
				
				if(strings.length != 15)
					continue;
				carInfoCache.put(key, strings);
			}
		}catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
			return false;
		}
		
		return true;
	}
	
	private synchronized static boolean loadLastrecordByRediscluster(){
		boolean keyLoadComp=false;
		ExecutorService threadPool=Executors.newCachedThreadPool();
		CTFOCacheKeys ctfoCacheKeys=null;
        try {
            ctfoCacheKeys = CTFOUtils.getDefaultCTFOCacheTable().getCTFOCacheKeys();
            List<KeysLoader> executors=new LinkedList<KeysLoader>();
            while(ctfoCacheKeys.next()){
            	List<String>keys=ctfoCacheKeys.getKeys();
            	executors.add(new KeysLoader(keys));
            }
            for (KeysLoader executor : executors) {
				threadPool.execute(executor);
			}
            boolean exe=true;
            while(exe){
            	boolean allComplete=true;
            	for (KeysLoader executor : executors) {
					if(!executor.isComplete()){
						allComplete=false;
						TimeUnit.MILLISECONDS.sleep(100);
						break;
					}
				}
            	if (allComplete) 
            		exe=false;
            }
            keyLoadComp = true;
            ctfoCacheKeys=null;
            executors=null;
        } catch (Exception e) {
            System.out.println("--------redis集群初始化实时数据计算异常！" + e);
            e.printStackTrace();
        }
        return keyLoadComp;
	}
	
	private static class KeysLoader implements Runnable,Serializable{

		/**
		 * 
		 */
		private static final long serialVersionUID = 112345600014L;
		private List<String>keys;
		private boolean complete;
		public KeysLoader(List<String> keys) {
			super();
			this.keys = keys;
			complete = false;
		}

		@Override
		public void run() {

			if(null!=keys && keys.size()>0)
				loadBykeys(keys);
			complete = true;
		}
		
		/**
		 * 从redis 集群中获取数据用于统计
		 * @param keys
		 */
		void loadBykeys(List<String> keys){
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
					Map<String, String> newmap =  new TreeMap<String, String>();
					//不缓存无用的数据项，减小缓存大小
					for (Map.Entry<String, String> entry : map.entrySet()) {
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
					carVids.offer(key);
					carlastrecord.put(key, newmap);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public boolean isComplete() {
			return complete;
		}
	}
	
	/**
	 * 重启的时候获取集群中车辆最后一条数据
	 * @return
	 */
	public synchronized static Cache<String,Map<String,String>> getDataCache(){
		try {
			if (!redisclusterIsload) 
				initDatByCluster();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return carlastrecord;
	}
	
	public synchronized static Cache<String, String[]> getCarinfoCache(){
		try {
			if (!carinfoIsload) 
				initCarinfoCache();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return carInfoCache;
	}
	
	static void destory(){
		carlastrecord = null;
		carInfoCache = null;
		redisclusterIsload = false;
		carinfoIsload = false;
	}

	public static void main(String[] args) {
		System.out.println(getDataCache().size());
	}
}
