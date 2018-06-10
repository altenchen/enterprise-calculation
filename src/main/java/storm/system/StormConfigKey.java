package storm.system;

/**
 * @author: xzp
 * @date: 2018-06-10
 * @description: org.apache.storm.Config 中用到的键
 */
public final class StormConfigKey {
    /**
     * 多长时间算是离线
     */
    public static final String REDIS_OFFLINE_SECOND = "redis.offline.time";

    public static final String REDIS_OFFLINE_CHECK_SPAN_SECOND = "redis.offline.checkTime";
    
    public static final String REDIS_CLUSTER_DATA_SYN = "redis.cluster.data.syn";
}
