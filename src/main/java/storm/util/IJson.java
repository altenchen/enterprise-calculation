package storm.util;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-07-03
 * @description:
 */
public interface IJson {

    /**
     * 序列化
     * @param object 对象
     * @param <T> 对象类型
     * @return json字符串
     */
    default <T> String toJson(T object) {
        final Gson gson = new Gson();
        final String json = gson.toJson(object);
        return json;
    }

    /**
     * 反序列化
     * @param json json字符串
     * @param <R> 对象类型
     * @return 对象
     */
    default <R> Map<String, R> parseMap(String json) {
        final Gson gson = new Gson();
        final Map<String, R> map = gson.fromJson(
            json,
            new TypeToken<Map<String, R>>() {
            }.getType());
        return map;
    }
}
