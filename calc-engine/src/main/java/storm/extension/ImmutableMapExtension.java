package storm.extension;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
     * @return 过滤后的不可变字典
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

    /**
     * 字典合并
     * @param base 基础字典
     * @param overlay 覆盖字典
     * @param <K> 键类型
     * @param <V> 值类型
     * @return 合并后的不可变字典
     */
    @NotNull
    public static <K, V> ImmutableMap<K, V> union(
        @Nullable final Map<? extends K, ? extends V> base,
        @Nullable final Map<? extends K, ? extends V> overlay) {

        final Map<K, V> result = Maps.newHashMap();
        Optional.ofNullable(overlay).ifPresent(map ->{
            if (MapUtils.isNotEmpty(map)) {
                map.forEach((k,v) -> {
                    if(null == k || null == v){
                        return;
                    }
                    result.put(k, v);
                });
            }
        });
        Optional.ofNullable(base).ifPresent(map ->{
            if (MapUtils.isNotEmpty(map)) {
                map.forEach((k,v) -> {
                    if(null == k || null == v || result.containsKey(k)){
                        return;
                    }
                    result.put(k, v);
                });
            }
        });
        return ImmutableMap.copyOf(result);
    }

    /**
     * 字典合并
     * @param base 基础字典
     * @param overlay 覆盖字典
     * @param <K> 键类型
     * @param <V> 值类型
     * @return 合并后的不可变字典
     */
    @NotNull
    public static <K, V> ImmutableMap<K, V> union(
        @Nullable final ImmutableMap<? extends K, ? extends V> base,
        @Nullable final ImmutableMap<? extends K, ? extends V> overlay) {

        final Map<K, V> result = Maps.newHashMap();
        Optional.ofNullable(base).ifPresent(result::putAll);
        Optional.ofNullable(overlay).ifPresent(result::putAll);
        return ImmutableMap.copyOf(result);
    }
}
