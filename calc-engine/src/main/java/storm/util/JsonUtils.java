package storm.util;

import com.alibaba.fastjson.JSON;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.function.Function;

/**
 * Json工具类, 用于统一配置.
 *
 * @author: xzp
 * @date: 2018-07-07
 * @description:
 */
public final class JsonUtils {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);

    private static final JsonUtils INSTANCE = new JsonUtils();

    @Contract(pure = true)
    public static JsonUtils getInstance() {
        return INSTANCE;
    }

    private JsonUtils() {
        if (INSTANCE != null) {
            throw new IllegalStateException();
        }
    }

    @NotNull
    public final <T> String toJson(final T src) {
        return JSON.toJSONString(src);
    }

    @Contract("null, _ -> null")
    public final <T> T fromJson(final String json, final Type typeOfT) {
        return JSON.parseObject(json, typeOfT);
    }

    public final <T> T fromJson(
        final String json,
        final Type typeOfT,
        @NotNull final Function<@NotNull Exception, @Nullable ? extends T> exceptionFunction) {
        try {
            return JSON.parseObject(json, typeOfT);
        } catch (final Exception e) {
            return exceptionFunction.apply(e);
        }
    }

    @Contract("null, _ -> null")
    public final <T> T fromJson(final String json, final Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

}
