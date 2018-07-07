package storm.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jetbrains.annotations.Contract;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

/**
 * Gson工具类, 用于统一配置.
 * @author: xzp
 * @date: 2018-07-07
 * @description:
 */
public final class GsonUtils {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(GsonUtils.class);

    private static final GsonUtils INSTANCE = new GsonUtils();

    @Contract(pure = true)
    public static GsonUtils getInstance() {
        return INSTANCE;
    }

    private final Gson gson = new GsonBuilder()
        .setDateFormat("yyyyMMddHHmmss")
        .create();

    private GsonUtils() {
        if (INSTANCE != null) {
            throw new IllegalStateException();
        }
    }

    @Contract(pure = true)
    public final Gson getGson() {
        return gson;
    }

    @Contract("null, _ -> null")
    public final <T> String toJson(T src) {
        return gson.toJson(src);
    }

    /**
     * @param src
     * @param typeOfSrc new TypeToken<T>() {}.getType()
     * @param <T>
     * @return
     */
    @Contract("null, _ -> null")
    public final <T> String toJson(T src, Type typeOfSrc) {
        return gson.toJson(src, typeOfSrc);
    }

    /**
     * @param json
     * @param typeOfT new TypeToken<T>() {}.getType()
     * @param <T>
     * @return
     */
    @Contract("null, _ -> null")
    public final <T> T fromJson(String json, Type typeOfT) {
        return gson.fromJson(json, typeOfT);
    }

    @Contract("null, _ -> null")
    public final <T> T fromJson(String json, Class<T> classOfT) {
        return gson.fromJson(json, classOfT);
    }
}
