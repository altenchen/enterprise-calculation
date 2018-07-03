package storm.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;
import storm.util.ConfigUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 车辆上下文缓存
 * @author: xzp
 * @date: 2018-07-02
 * @description:
 */
public final class VehicleDataStreamCache {

    private static final int REDIS_DB_INDEX = 6;

    @NotNull
    private static final String buildTable(@NotNull String vid) {
        return "veh.data.stream." + vid;
    }

    /**
     * <vid, <key, value>>
     */
    private static final Cache<String, Map<String, String>> cacheTable;

    static {

        cacheTable = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Map<String, String>>() {
                @Override
                public Map<String, String> load(String s) {
                    return null;
                }

                @Override
                public Map<String, Map<String, String>> loadAll(Iterable<? extends String> keys) throws Exception {
                    return super.loadAll(keys);
                }
            });
    }
}
