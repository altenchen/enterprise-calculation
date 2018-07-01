package storm.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import storm.util.ConfigUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 车辆上下文缓存
 * @author: xzp
 * @date: 2018-07-02
 * @description:
 */
public final class VehicleContextCache {

    private static final ConfigUtils configUtils = ConfigUtils.getInstance();


    /**
     * <vin, <key, value>>
     */
    private static final Cache<String, Map<String, String>> cache;

    static {

        cache = CacheBuilder.newBuilder().build();
    }
}
