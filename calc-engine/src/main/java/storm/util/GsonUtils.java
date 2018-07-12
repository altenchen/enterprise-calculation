package storm.util;

import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

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
        .registerTypeAdapterFactory(ObjectTypeAdapter.FACTORY)
        .registerTypeAdapter(
            new TypeToken<TreeMap<String, Object>>() {
            }.getType(),
            (JsonDeserializer<TreeMap<String, Object>>) (jsonElement, type, jsonDeserializationContext) -> {

                final JsonObject jsonObject = jsonElement.getAsJsonObject();
                final Set<Map.Entry<String, JsonElement>> entrySet = jsonObject.entrySet();

                final TreeMap<String, Object> treeMap = new TreeMap<>();
                for (Map.Entry<String, JsonElement> entry : entrySet) {

                    final String key = entry.getKey();
                    final JsonElement element = entry.getValue();

                    treeMap.put(
                        key,
                        deserialize(element));
                }
                return treeMap;
            }
        )
        .registerTypeAdapter(
            new TypeToken<HashMap<String, Object>>() {
            }.getType(),
            (JsonDeserializer<HashMap<String, Object>>) (jsonElement, type, jsonDeserializationContext) -> {

                final JsonObject jsonObject = jsonElement.getAsJsonObject();
                final Set<Map.Entry<String, JsonElement>> entrySet = jsonObject.entrySet();

                final HashMap<String, Object> treeMap = new HashMap<>(entrySet.size());
                for (Map.Entry<String, JsonElement> entry : entrySet) {

                    final String key = entry.getKey();
                    final JsonElement element = entry.getValue();

                    treeMap.put(
                        key,
                        deserialize(element));
                }
                return treeMap;
            }
        )
        .registerTypeAdapter(
            new TypeToken<Object[]>() {
            }.getType(),
            (JsonDeserializer<Object[]>) (jsonElement, type, jsonDeserializationContext) -> {

                final JsonArray jsonArray = jsonElement.getAsJsonArray();

                final Collection<Object> linkedList = new LinkedList<>();

                for (JsonElement element : jsonArray) {
                    linkedList.add(deserialize(element));
                }
                return linkedList.toArray();
            }
        )
        .registerTypeHierarchyAdapter(
            Map.class,
            (JsonDeserializer<Map<String, Object>>) (jsonElement, type, jsonDeserializationContext) -> {

                final JsonObject jsonObject = jsonElement.getAsJsonObject();
                final Set<Map.Entry<String, JsonElement>> entrySet = jsonObject.entrySet();

                final LinkedTreeMap<String, Object> linkedMap = new LinkedTreeMap<>();
                for (Map.Entry<String, JsonElement> entry : entrySet) {

                    final String key = entry.getKey();
                    final JsonElement element = entry.getValue();

                    linkedMap.put(
                        key,
                        deserialize(element));
                }
                return linkedMap;
            }
        )
        .registerTypeHierarchyAdapter(
            Iterable.class,
            (JsonDeserializer<Iterable<Object>>) (jsonElement, type, jsonDeserializationContext) -> {

                final JsonArray jsonArray = jsonElement.getAsJsonArray();

                final Collection<Object> linkedList = new LinkedList<>();

                for (JsonElement element : jsonArray) {
                    linkedList.add(deserialize(element));
                }
                return linkedList;
            }
        )
        .create();

    @Nullable
    private static Object deserialize(@NotNull JsonElement jsonElement)
        throws JsonParseException {

        if (jsonElement.isJsonPrimitive()) {

            final JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();

            if (primitive.isBoolean()) {
                return primitive.getAsBoolean();
            }

            final String asString = primitive.getAsString();

            if (primitive.isNumber()) {

                if (NumberUtils.isDigits(asString)) {

                    return primitive.getAsInt();
                } else {

                    return primitive.getAsDouble();
                }
            }

            return asString;
        }

        if (jsonElement.isJsonObject()) {
            return jsonElement.getAsJsonObject();
        }

        if (jsonElement.isJsonArray()) {
            return jsonElement.getAsJsonArray();
        }

        return jsonElement.getAsJsonNull();
    }

    private GsonUtils() {
        if (INSTANCE != null) {
            throw new IllegalStateException();
        }
    }

    @Contract(pure = true)
    public final Gson getGson() {
        return gson;
    }

    public final <T> String toJson(T src) {
        return gson.toJson(src);
    }

    public final <T> String toJson(T src, Type typeOfSrc) {
        return gson.toJson(src, typeOfSrc);
    }

    @Contract("null, _ -> null")
    public final <T> T fromJson(String json, Type typeOfT) {
        return gson.fromJson(json, typeOfT);
    }

    @Contract("null, _ -> null")
    public final <T> T fromJson(String json, Class<T> classOfT) {
        return gson.fromJson(json, classOfT);
    }

    private static final class ObjectTypeAdapter extends TypeAdapter<Object> {

        static final TypeAdapterFactory FACTORY = new TypeAdapterFactory() {
            @SuppressWarnings("unchecked")
            @Override public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                if (type.getRawType() == Object.class) {
                    return (TypeAdapter<T>) new ObjectTypeAdapter(gson);
                }
                return null;
            }
        };

        private final Gson gson;

        ObjectTypeAdapter(Gson gson) {
            this.gson = gson;
        }

        @Override public Object read(JsonReader in) throws IOException {
            JsonToken token = in.peek();
            switch (token) {
                case BEGIN_ARRAY:
                    List<Object> list = new ArrayList<>();
                    in.beginArray();
                    while (in.hasNext()) {
                        list.add(read(in));
                    }
                    in.endArray();
                    return list;

                case BEGIN_OBJECT:
                    Map<String, Object> map = new LinkedTreeMap<>();
                    in.beginObject();
                    while (in.hasNext()) {
                        map.put(in.nextName(), read(in));
                    }
                    in.endObject();
                    return map;

                case STRING:
                    return in.nextString();

                case NUMBER:
                    final String nextString = in.nextString();
                    if(NumberUtils.isDigits(nextString)) {
                        final long longvalue = NumberUtils.toLong(nextString);
                        if(longvalue < Integer.MIN_VALUE || longvalue > Integer.MAX_VALUE) {
                            return longvalue;
                        }
                        return (int)longvalue;
                    } else {
                        NumberUtils.toDouble(nextString);
                    }

                case BOOLEAN:
                    return in.nextBoolean();

                case NULL:
                    in.nextNull();
                    return null;

                default:
                    throw new IllegalStateException();
            }
        }

        @SuppressWarnings("unchecked")
        @Override public void write(JsonWriter out, Object value) throws IOException {
            if (value == null) {
                out.nullValue();
                return;
            }

            TypeAdapter<Object> typeAdapter = gson.getAdapter((Class<Object>) value.getClass());
            if (typeAdapter instanceof ObjectTypeAdapter) {
                out.beginObject();
                out.endObject();
                return;
            }

            typeAdapter.write(out, value);
        }
    }

}
