package storm.dto.alarm;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.ObjectExtension;
import storm.system.SysDefine;
import storm.extension.ImmutableMapExtension;
import storm.util.ConfigUtils;
import storm.util.SqlUtils;

/**
 * @author wza
 * 预警规则获取
 */
public class EarlyWarnsGetter {

    private static final Logger LOG = LoggerFactory.getLogger(EarlyWarnsGetter.class);

    private static final SqlUtils SQL_UTILS = SqlUtils.getInstance();

    private static final String ALL = "ALL";

    /**
     * 平台报警规则查询 SQL
     */
    private static final String ALARM_RULE_SQL;

    /**
     * 数据库查询最小间隔
     */
    private static final long DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND;

    /**
     * 最近一次从数据库更新的时间
     */
    private static long lastRebuildTime = 0;

    static {
        final ConfigUtils configUtils = ConfigUtils.getInstance();
        final Properties sysParams = configUtils.sysParams;
        final Properties sysDefine = configUtils.sysDefine;

        ALARM_RULE_SQL = StringUtils.defaultIfEmpty(
            sysParams.getProperty("alarm.rule.sql"),
            "select id,name,l1_seq_no,is_last1,l2_seq_no,is_last2,expr_left,r1_val,r2_val,expr_mid,levels,ifnull(veh_model_id,'ALL') from sys_data_const where is_valid=1 and type=1 and (depend_id is null or depend_id = '')"
        );
        LOG.debug("平台报警规则数据库查询语句为[{}]", ALARM_RULE_SQL);

        DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND = TimeUnit.SECONDS.toMillis(
            NumberUtils.toLong(
                sysDefine.getProperty(
                    SysDefine.DB_CACHE_FLUSH_TIME_SECOND),
                60
            )
        );
        LOG.debug("平台报警规则数据库更新最小间隔为[{}]毫秒", DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND);
    }

    @NotNull
    private static ImmutableMap<String, ImmutableMap<String, EarlyWarn>> rules = ImmutableMap.of();

    private static synchronized void rebuild(final long currentTimeMillis) {

        if(currentTimeMillis - lastRebuildTime > DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND) {

            try {

                LOG.info("平台报警规则重构开始.");

                rules = buildEarlyWarnFromDb();

                LOG.info("平台报警规则重构完毕.");

                lastRebuildTime = currentTimeMillis;
            } catch (final Exception e) {
                LOG.warn("平台报警规则重构异常", e);
            }

        }
    }

    @NotNull
    private static ImmutableMap<String, ImmutableMap<String, EarlyWarn>> buildEarlyWarnFromDb() {
        return ObjectExtension.defaultIfNull(
            SQL_UTILS.query(ALARM_RULE_SQL, resultSet -> {
                final HashMap<String, EarlyWarn> items = Maps.newHashMapWithExpectedSize(100);

                while (resultSet.next()) {

                    final String ruleId = resultSet.getString(1);
                    if (StringUtils.isBlank(ruleId)) {
                        LOG.warn("空白的平台报警规则ID");
                        continue;
                    }
                    final String ruleName = resultSet.getString(2);

                    if (items.containsKey(ruleId)) {
                        LOG.warn("重复的平台报警规则[{}]([{}],[{}])", ruleId, ruleName, items.get(ruleId).ruleName);
                        continue;
                    }

                    final String left1DataKey = resultSet.getString(3);
                    if (StringUtils.isBlank(left1DataKey)) {
                        LOG.warn("平台报警规则[{}][{}]空白的左一数据键", ruleId, ruleName);
                        continue;
                    }
                    final boolean left1UsePrev = StringUtils.equals("1", resultSet.getString(4));
                    final String left2DataKey = resultSet.getString(5);
                    final boolean left2UsePrev = StringUtils.equals("1", resultSet.getString(6));
                    final String arithmeticExpression = resultSet.getString(7);
                    final String right1Value = resultSet.getString(8);
                    if (StringUtils.isBlank(right1Value)) {
                        LOG.warn("平台报警规则[{}][{}]空白的右一数据值", ruleId, ruleName);
                        continue;
                    }
                    final String right2Value = resultSet.getString(9);
                    final String logicExpression = resultSet.getString(10);
                    if (StringUtils.isBlank(logicExpression)) {
                        LOG.warn("平台报警规则[{}][{}]空白的逻辑运算符", ruleId, ruleName);
                        continue;
                    }

                    final BiFunction<ImmutableMap<String, String>, ImmutableMap<String, String>, Boolean> function =
                        EarlyWarn.buildFunction(
                            ruleId, ruleName,
                            left1DataKey, left1UsePrev, left2DataKey, left2UsePrev, arithmeticExpression,
                            right1Value, right2Value, logicExpression
                        );
                    if (null == function) {
                        LOG.warn("平台报警规则[{}][{}]无法构建表达式函数", ruleId, ruleName);
                        continue;
                    }

                    final int level = ((Function<String, Integer>) s -> {
                        if (NumberUtils.isDigits(s)) {
                            return NumberUtils.toInt(s);
                        }
                        LOG.warn("平台报警规则[{}][{}]报警等级[{}]非数字, 使用0表示的动态等级替代.", ruleId, ruleName, s);
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

    public static ImmutableMap<String, EarlyWarn> getRulesByVehicleModel(@Nullable final String vehicleModel) {

        final long currentTimeMillis = System.currentTimeMillis();
        if(currentTimeMillis - lastRebuildTime > DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND) {
            rebuild(currentTimeMillis);
        }

        return getRulesByVehicleModelWithoutRebuild(rules, vehicleModel);
    }

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
