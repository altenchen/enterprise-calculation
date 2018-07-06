package storm.util;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

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

    @Nullable
    @Override
    public final <T> String toJson(@Nullable final T object) {
        final Gson gson = new Gson();
        final String json = gson.toJson(object);
        return json;
    }

    @Nullable
    @Override
    public  <R> Map<String, R> parseMap(String json) {
        final Gson gson = new Gson();
        final Map<String, R> map = gson.fromJson(
            json,
            new TypeToken<Map<String, R>>() {
            }.getType());
        return map;
    }
}
