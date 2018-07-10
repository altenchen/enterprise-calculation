package storm.handler.cal;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import storm.dao.DataToRedis;
import storm.dao.RedisClusterOldUtil;
import storm.dao.RedisClusterOldPool;

public class RedisClusterLoaderOld {

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
    private static final String DATA_KEY = "xny-realInfo-*";
    private synchronized static void initDatByCluster(){
        redisclusterIsload = loadLastrecordByRediscluster();
    }

    private synchronized static void initCarinfoCache(){
        carinfoIsload = loadCarinfoCache();
    }

    /**
     *
     * @return
     */
    private synchronized static boolean loadCarinfoCache(){
        try {
            DataToRedis redis=new DataToRedis();
            Map<String, String> map = redis.hgetallMapByKeyAndDb("XNY.CARINFO", 0);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                if (StringUtils.isEmpty(key)
                        || StringUtils.isEmpty(value))
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

    /**
     *
     * @return
     */
    private synchronized static boolean loadLastrecordByRediscluster(){
        boolean keyLoadComp=false;
        ExecutorService threadPool=Executors.newCachedThreadPool();
        List<RedisClusterOldPool> pools=null;
        try {
            pools = RedisClusterOldUtil.getPools();
            List<KeysLoaderOld> executors=new LinkedList<KeysLoaderOld>();
            Iterator<RedisClusterOldPool> iterator = pools.iterator();
            while(iterator.hasNext()){
                RedisClusterOldPool pool = iterator.next();
                executors.add(new KeysLoaderOld(pool));
            }
            for (KeysLoaderOld executor : executors) {
                threadPool.execute(executor);
            }
            boolean exe=true;
            while(exe){
                boolean allComplete=true;
                for (KeysLoaderOld executor : executors) {
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
            pools=null;
            executors=null;
        } catch (Exception e) {
            System.out.println("--------redis集群初始化实时数据计算异常！" + e);
            e.printStackTrace();
        }
        return keyLoadComp;
    }

    private static class KeysLoaderOld implements Runnable,Serializable{

        /**
         *
         */
        private static final long serialVersionUID = 112345600014L;
        private RedisClusterOldPool pool;
        private boolean complete;
        KeysLoaderOld(RedisClusterOldPool pool) {
            super();
            this.pool = pool;
            complete = false;
        }

        @Override
        public void run() {

            if (null != pool) {
                Set<String> keys = pool.keys(0, DATA_KEY);
                if(null!=keys && keys.size()>0)
                    loadBykeys(keys);
            }
            complete = true;
        }

        /**
         * 从redis 集群中获取数据用于统计
         * @param keys
         */
        void loadBykeys(Set<String> keys){
            try {
                for(String key :keys){
                    if (StringUtils.isEmpty(key))
                        continue;
                    Map<String, String> map = pool.hgetall(0, key);
                    if(MapUtils.isEmpty(map))
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
