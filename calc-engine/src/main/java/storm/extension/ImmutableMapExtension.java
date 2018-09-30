package storm.extension;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

/**
 * @author: xzp
 * @date: 2018-09-29
 * @description:
 */
public final class ImmutableMapExtension {

    /**
     * 字典过滤
     *
     * @param map 数据源
     * @param filterFunction 过滤函数
     * @param <K> 键类型
     * @param <V> 值类型
     * @return 分组后的不可变字典
     */
    @NotNull
    public static <K, V> ImmutableMap<K, V> filter(
        @NotNull final Map<? extends K, ? extends V> map,
        @NotNull final BiPredicate<? super K, ? super V> filterFunction) {

        final ImmutableMap.Builder<K, V> builder = new ImmutableMap.Builder<>();
        map.forEach((k, v) -> {
            if (filterFunction.test(k, v)) {
                builder.put(k, v);
            }
        });

        return builder.build();
    }

    /**
     * 字典分组
     *
     * @param map 数据源
     * @param groupFunction 摘取分组标识的函数
     * @param <K> 键类型
     * @param <V> 值类型
     * @param <G> 组类型
     * @return 分组后的不可变字典
     */
    @NotNull
    public static <K, V, G> ImmutableMap<G, ImmutableMap<K, V>> group(
        @NotNull final Map<? extends K, ? extends V> map,
        @NotNull final BiFunction<? super K, ? super V, G> groupFunction) {

        final Map<G, Map<K, V>> groupMap = Maps.newHashMap();
        map.forEach((k, v) -> {
            final G group = groupFunction.apply(k, v);
            final Map<K, V> kvMap = groupMap.computeIfAbsent(group, g -> Maps.newHashMap());
            kvMap.put(k, v);
        });

        final ImmutableMap.Builder<G, ImmutableMap<K, V>> builder = new ImmutableMap.Builder<>();
        groupMap.forEach((g, kv) -> {
            builder.put(g, ImmutableMap.copyOf(kv));
        });

        return builder.build();
    }
}
