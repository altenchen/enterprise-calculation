package storm.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.apache.commons.lang.ObjectUtils;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


/**
 * @author: xzp
 * @date: 2018-06-22
 * @description: 车型缓存
 */
public class VehicleModelCache {

    private static final Logger logger = LoggerFactory.getLogger(VehicleModelCache.class);

    private static final VehicleModelCache INSTANCE = new VehicleModelCache();

    @Contract(pure = true)
    public static VehicleModelCache getInstance() {
        return INSTANCE;
    }

    private final Cache<String, String> cache;

    private VehicleModelCache() {
        if (INSTANCE != null) {
            throw new IllegalStateException();
        }

        cache = CacheBuilder.newBuilder()
            .expireAfterWrite(60, TimeUnit.MINUTES)
            .refreshAfterWrite(30, TimeUnit.MINUTES)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build(new CacheLoader<String, String>() {
                @Override
                public String load(String key) throws Exception {
                    // TODO XZP: 从数据库获取
                    return null;
                }
            });
    }

    @NotNull
    public String getVehicleModel(@NotNull String vid) {
        final String value = cache.getIfPresent(vid);
        return StringUtils.defaultString(value, "");
    }
}
