package storm.util;

import com.alibaba.fastjson.JSON;
import org.jetbrains.annotations.Contract;

/**
 * @author: xzp
 * @date: 2018-07-03
 * @description:
 */
public final class JsonUtils implements IJson {
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

    @Override
    public final String toJson(Object object) {
        return JSON.toJSONString(object);
    }

}
