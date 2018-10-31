package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.ImmutableMapExtension;
import storm.extension.ObjectExtension;
import storm.util.ConfigUtils;
import storm.util.SqlUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author wza
 * 预警规则获取
 */
public class EarlyWarnsGetter {

    private static final Logger LOG = LoggerFactory.getLogger(EarlyWarnsGetter.class);

    private static final SqlUtils SQL_UTILS = SqlUtils.getInstance();

    public static final String ALL = "ALL";

    /**
     * 最近一次从数据库更新的时间
     */
    private static long lastRebuildTime = 0;

    static {
        LOG.info("平台报警规则数据库查询语句为 {} ", ConfigUtils.getSysParam().getAlarmRuleSql());
        LOG.info("平台报警规则数据库更新最小间隔为 {} 毫秒", TimeUnit.SECONDS.toMillis(ConfigUtils.getSysDefine().getDbCacheFlushTime()));
    }

    /**
     * <vehicleType, <ruleId, rule>>
     */
    @NotNull
    private static ImmutableMap<String, ImmutableMap<String, EarlyWarn>> rules = ImmutableMap.of();

    private static synchronized void rebuild(final long currentTimeMillis) {

        long DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND = TimeUnit.SECONDS.toMillis(ConfigUtils.getSysDefine().getDbCacheFlushTime());
        if(currentTimeMillis - lastRebuildTime > DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND) {

            try {

                LOG.info("平台报警规则重构开始.");

                rules = buildEarlyWarnFromDb();

                LOG.info("平台报警规则重构完毕, 共获取到[{}]条规则.", rules.values().stream().mapToInt(Map::size).sum());

                lastRebuildTime = currentTimeMillis;
            } catch (final Exception e) {
                LOG.warn("平台报警规则重构异常", e);
            }

        }
    }

    @NotNull
    private static ImmutableMap<String, ImmutableMap<String, EarlyWarn>> buildEarlyWarnFromDb() {
        return ObjectExtension.defaultIfNull(
            SQL_UTILS.query(ConfigUtils.getSysParam().getAlarmRuleSql(), resultSet -> {
                final HashMap<String, EarlyWarn> items = Maps.newHashMapWithExpectedSize(100);

                while (resultSet.next()) {

                    final String ruleId = resultSet.getString(1);
                    if (StringUtils.isBlank(ruleId)) {
                        LOG.warn("空白的平台报警规则ID");
                        continue;
                    }
                    final String ruleName = resultSet.getString(2);

                    if (items.containsKey(ruleId)) {
                        LOG.warn("重复的平台报警规则 RULE_ID:{} ROLE_NAME:{} --> {})", ruleId, ruleName, items.get(ruleId).ruleName);
                        continue;
                    }

                    final String left1DataKey = resultSet.getString(3);
                    if (StringUtils.isBlank(left1DataKey)) {
                        LOG.warn("平台报警规则 RULE_ID:{} ROLE_NAME:{} 空白的左一数据键", ruleId, ruleName);
                        continue;
                    }
                    final boolean left1UsePrev = StringUtils.equals("1", resultSet.getString(4));
                    final String left2DataKey = resultSet.getString(5);
                    final boolean left2UsePrev = StringUtils.equals("1", resultSet.getString(6));
                    final String arithmeticExpression = resultSet.getString(7);
                    final String right1Value = resultSet.getString(8);
                    if (StringUtils.isBlank(right1Value)) {
                        LOG.warn("平台报警规则 RULE_ID:{} ROLE_NAME:{} 空白的右一数据值", ruleId, ruleName);
                        continue;
                    }
                    final String right2Value = resultSet.getString(9);
                    final String logicExpression = resultSet.getString(10);
                    if (StringUtils.isBlank(logicExpression)) {
                        LOG.warn("平台报警规则 RULE_ID:{} ROLE_NAME:{} 空白的逻辑运算符", ruleId, ruleName);
                        continue;
                    }

                    final BiFunction<ImmutableMap<String, String>, ImmutableMap<String, String>, Boolean> function =
                        EarlyWarn.buildFunction(
                            ruleId, ruleName,
                            left1DataKey, left1UsePrev, left2DataKey, left2UsePrev, arithmeticExpression,
                            right1Value, right2Value, logicExpression
                        );
                    if (null == function) {
                        LOG.warn("平台报警规则 RULE_ID:{} ROLE_NAME:{} 无法构建表达式函数", ruleId, ruleName);
                        continue;
                    }

                    final int level = ((Function<String, Integer>) s -> {
                        if (NumberUtils.isDigits(s)) {
                            return NumberUtils.toInt(s);
                        }
                        LOG.warn("平台报警规则 RULE_ID:{} ROLE_NAME:{} 报警等级 {} 非数字, 使用0表示的动态等级替代.", ruleId, ruleName, s);
                        return 0;
                    }).apply(resultSet.getString(11));
                    final String vehicleModelId = ((Function<String, String>) s -> {
                        if (StringUtils.isNotBlank(s)) {
                            return s;
                        }
                        return ALL;
                    }).apply(resultSet.getString(12));

                    final EarlyWarn earlyWarn = new EarlyWarn(
                        ruleId, ruleName,
                        left1DataKey, left1UsePrev, left2DataKey, left2UsePrev, arithmeticExpression,
                        right1Value, right2Value, logicExpression,
                        function,
                        level,
                        vehicleModelId);

                    items.put(ruleId, earlyWarn);
                }

                return ImmutableMapExtension.group(items, (k, v) -> v.vehicleModelId);
            }),
            ImmutableMap::of);
    }

    @NotNull
    public static ImmutableMap<String, ImmutableMap<String, EarlyWarn>> getAllRules() {

        final long currentTimeMillis = System.currentTimeMillis();
        long DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND = TimeUnit.SECONDS.toMillis(ConfigUtils.getSysDefine().getDbCacheFlushTime());
        if(currentTimeMillis - lastRebuildTime > DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND) {
            rebuild(currentTimeMillis);
        }

        return rules;
    }

    @NotNull
    public static ImmutableMap<String, EarlyWarn> getRulesByVehicleModel(@Nullable final String vehicleModel) {

        final long currentTimeMillis = System.currentTimeMillis();
        long DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND = TimeUnit.SECONDS.toMillis(ConfigUtils.getSysDefine().getDbCacheFlushTime());
        if(currentTimeMillis - lastRebuildTime > DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND) {
            rebuild(currentTimeMillis);
        }

        return getRulesByVehicleModelWithoutRebuild(rules, vehicleModel);
    }

    @NotNull
    private static ImmutableMap<String, EarlyWarn> getRulesByVehicleModelWithoutRebuild(
        @NotNull final ImmutableMap<String, ImmutableMap<String, EarlyWarn>> warns,
        @Nullable final String vehicleModel) {

        final ImmutableMap.Builder<String, EarlyWarn> builder = new ImmutableMap.Builder<>();
        if(!StringUtils.equals(ALL, vehicleModel)) {
            final ImmutableMap<String, EarlyWarn> common = warns.get(ALL);
            if(MapUtils.isNotEmpty(common)) {
                builder.putAll(common);
            }
        }
        final ImmutableMap<String, EarlyWarn> spec = warns.get(vehicleModel);
        if(MapUtils.isNotEmpty(spec)) {
            builder.putAll(spec);
        }
        return builder.build();
    }

}
