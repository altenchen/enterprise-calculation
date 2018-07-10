package storm.util;

import com.google.common.reflect.TypeToken;
import com.google.gson.*;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Gson工具类, 用于统一配置.
 *
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
        .registerTypeAdapter(
            new TypeToken<TreeMap<String, Object>>() {
            }.getType(),
            new JsonDeserializer<TreeMap<String, Object>>() {

                @Override
                public TreeMap<String, Object> deserialize(
                    JsonElement jsonElement,
                    Type type,
                    JsonDeserializationContext jsonDeserializationContext)
                    throws JsonParseException {

                    final TreeMap<String, Object> treeMap = new TreeMap<>();

                    final JsonObject jsonObject = jsonElement.getAsJsonObject();
                    final Set<Map.Entry<String, JsonElement>> entrySet = jsonObject.entrySet();
                    for (Map.Entry<String, JsonElement> entry : entrySet) {

                        final String key = entry.getKey();
                        final JsonElement element = entry.getValue();

                        if (element.isJsonPrimitive()) {

                            final JsonPrimitive primitive = element.getAsJsonPrimitive();
                            if (primitive.isBoolean()) {

                                treeMap.put(key, primitive.getAsBoolean());
                            } else {

                                final String asString = primitive.getAsString();
                                if (primitive.isNumber()) {

                                    if (NumberUtils.isDigits(asString)) {

                                        final long asLong = primitive.getAsLong();
                                        if (asLong >= Byte.MIN_VALUE && asLong <= Byte.MAX_VALUE) {
                                            treeMap.put(key, (byte) asLong);
                                        } else if (asLong > Short.MIN_VALUE && asLong < Short.MAX_VALUE) {
                                            treeMap.put(key, (short) asLong);
                                        } else if (asLong > Integer.MIN_VALUE && asLong < Integer.MAX_VALUE) {
                                            treeMap.put(key, (int) asLong);
                                        } else {
                                            treeMap.put(key, primitive.getAsLong());
                                        }
                                    } else {

                                        final double asDouble = primitive.getAsDouble();
                                        if(asDouble >= Float.MIN_VALUE && asDouble <= Float.MAX_VALUE) {
                                            treeMap.put(key, (float)asDouble);
                                        } else {
                                            treeMap.put(key, asDouble);
                                        }
                                    }
                                } else {

                                    treeMap.put(key, asString);
                                }
                            }
                            continue;
                        }
                        treeMap.put(key, element);
                    }
                    return treeMap;
                }
            }
        )
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
