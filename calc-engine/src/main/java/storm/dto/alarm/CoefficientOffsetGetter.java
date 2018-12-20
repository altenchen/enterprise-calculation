package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.ObjectExtension;
import storm.util.ConfigUtils;
import storm.util.SqlUtils;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author 徐志鹏
 * 偏移系数自定义数据项处理
 * <p>
 * TODO: 目前从配置文件读, 等基础平台规划好, 将会改成从数据库读.
 */
public final class CoefficientOffsetGetter {

    @NotNull
    private static final Logger LOG = LoggerFactory.getLogger(CoefficientOffsetGetter.class);

    private static final SqlUtils SQL_UTILS = SqlUtils.getInstance();

    public static final String DEFAULT = "Default";

    /**
     * 最近一次从数据库更新的时间
     */
    private static long lastRebuildTime = 0;

    /**
     * <vehicleType, <dataKey, rule>>
     */
    @NotNull
    private static ImmutableMap<String, ImmutableMap<String, CoefficientOffset>> rules = ImmutableMap.of();

    private static synchronized void rebuild(final long currentTimeMillis) {
        long DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND = TimeUnit.SECONDS.toMillis(ConfigUtils.getSysDefine().getDbCacheFlushtime());
        if (currentTimeMillis - lastRebuildTime > DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND) {

            try {

                LOG.info("偏移系数规则重构开始.");

                rules = buildCoefficientOffsetFromDb();

                LOG.info("偏移系数规则重构完毕, 共获取到 {} 条规则.", rules.values().stream().mapToInt(Map::size).sum());

                lastRebuildTime = currentTimeMillis;
            } catch (final Exception e) {
                LOG.warn("偏移系数规则重构异常", e);
            }

        }
    }

    @NotNull
    private static ImmutableMap<String, ImmutableMap<String, CoefficientOffset>> buildCoefficientOffsetFromDb() {
        return ObjectExtension.defaultIfNull(
            SQL_UTILS.query(ConfigUtils.getSysParam().getDataOffsetCoefficientSql(), resultSet -> {
                final Map<String, Map<String, CoefficientOffset>> rules = Maps.newHashMapWithExpectedSize(100);
                final Set<String> duplicatedCheck = Sets.newHashSetWithExpectedSize(100);

                while (resultSet.next()) {

                    final String itemId = resultSet.getString(1);
                    if (StringUtils.isBlank(itemId)) {
                        LOG.warn("空白的偏移系数规则ID");
                        continue;
                    }
                    if (duplicatedCheck.contains(itemId)) {
                        LOG.warn("重复的偏移系数规则 {}", itemId);
                        continue;
                    }

                    final String dataKey = resultSet.getString(2);
                    if (StringUtils.isBlank(itemId)) {
                        LOG.warn("偏移系数规则 {}:空白的偏移系数数据键", itemId);
                        continue;
                    }

                    final BigDecimal dataOffset = resultSet.getBigDecimal(3);
                    final BigDecimal dataCoefficient = resultSet.getBigDecimal(4);
                    final int decimalPrecision = resultSet.getInt(5);
                    final int itemType = resultSet.getInt(6);
                    final boolean isSemaphore = 1 == itemType;
                    if (isSemaphore) {
                        final boolean isOffsetEqualZero = 0 == dataOffset.compareTo(BigDecimal.ZERO);
                        if (!isOffsetEqualZero) {
                            LOG.warn("偏移系数规则 {}:信号量偏移必须是0", itemId);
                            continue;
                        }
                        final boolean isCoefficientEqualOne = 0 == dataCoefficient.compareTo(BigDecimal.ONE);
                        if (!isCoefficientEqualOne) {
                            LOG.warn("偏移系数规则 {}:信号量系数必须是1", itemId);
                            continue;
                        }
                        final boolean isPrecisionEqualZero = 0 == decimalPrecision;
                        if (!isPrecisionEqualZero) {
                            LOG.warn("偏移系数规则 {}:信号量小数精度必须是0", itemId);
                            continue;
                        }
                    } else {
                        final boolean isAnalog = 2 == itemType;
                        if (!isAnalog) {
                            LOG.warn("偏移系数规则 {}:既不是信号量也不是模拟量", itemId);
                            continue;
                        }
                    }

                    final ImmutableSet<String> vehicleModelSet = ((Function<String, ImmutableSet<String>>) s -> {
                        final String[] parts = StringUtils.split(s, ',');
                        if (ArrayUtils.isNotEmpty(parts)) {
                            return ImmutableSet.copyOf(parts);
                        } else {
                            return ImmutableSet.of(DEFAULT);
                        }
                    }).apply(resultSet.getString(7));

                    final CoefficientOffset coefficientOffset = new CoefficientOffset(
                        itemId,
                        dataKey,
                        dataOffset,
                        dataCoefficient,
                        decimalPrecision
                    );

                    vehicleModelSet.forEach(vehicleModel -> {
                        final Map<String, CoefficientOffset> dataCoefficientOffset = rules.computeIfAbsent(
                            vehicleModel,
                            k -> Maps.newHashMap()
                        );
                        if (dataCoefficientOffset.containsKey(dataKey)) {
                            final CoefficientOffset duplicated = dataCoefficientOffset.get(dataKey);
                            LOG.warn("车型:{} 数据项:{} 重复的偏移系数 {}:{}", vehicleModel, dataKey, duplicated.getItemId(), itemId);
                        } else {
                            dataCoefficientOffset.put(dataKey, coefficientOffset);
                        }
                    });

                    duplicatedCheck.add(itemId);
                }

                final ImmutableMap.Builder<String, ImmutableMap<String, CoefficientOffset>> builder =
                    new ImmutableMap.Builder<>();
                rules.forEach(
                    (vehicleModel, dataCoefficientOffset) -> builder.put(
                        vehicleModel,
                        ImmutableMap.copyOf(dataCoefficientOffset)));
                return builder.build();
            }),
            ImmutableMap::of);
    }

    @NotNull
    public static ImmutableMap<String, ImmutableMap<String, CoefficientOffset>> getAllCoefficientOffsets() {

        final long currentTimeMillis = System.currentTimeMillis();
        long DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND = TimeUnit.SECONDS.toMillis(ConfigUtils.getSysDefine().getDbCacheFlushtime());
        if(currentTimeMillis - lastRebuildTime > DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND) {
            rebuild(currentTimeMillis);
        }

        return rules;
    }

    @Nullable
    public static CoefficientOffset getCoefficientOffsetByVehicleModel(
        @Nullable final String vehicleModel,
        @Nullable final String dataKey) {

        final long currentTimeMillis = System.currentTimeMillis();
        long DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND = TimeUnit.SECONDS.toMillis(ConfigUtils.getSysDefine().getDbCacheFlushtime());
        if (currentTimeMillis - lastRebuildTime > DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND) {
            rebuild(currentTimeMillis);
        }

        return getCoefficientOffsetByVehicleModelWithoutRebuild(rules, vehicleModel, dataKey);
    }

    @Nullable
    private static CoefficientOffset getCoefficientOffsetByVehicleModelWithoutRebuild(
        @NotNull final ImmutableMap<String, ImmutableMap<String, CoefficientOffset>> coefficientOffsets,
        @Nullable final String vehicleModel,
        @Nullable final String dataKey) {

        if (!StringUtils.equals(DEFAULT, vehicleModel)) {
            final ImmutableMap<String, CoefficientOffset> spec = coefficientOffsets.get(vehicleModel);
            if (MapUtils.isNotEmpty(spec)) {
                final CoefficientOffset coefficientOffset = spec.get(dataKey);
                if(null != coefficientOffset) {
                    return coefficientOffset;
                }
            }
        }

        final ImmutableMap<String, CoefficientOffset> common = coefficientOffsets.get(DEFAULT);
        if (MapUtils.isNotEmpty(common)) {
            return common.get(dataKey);
        }

        return null;
    }
}
