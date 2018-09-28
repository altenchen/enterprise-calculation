package storm.dto.alarm;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.storm.kafka.spout.Func;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.collect.ImmutableMapUtils;
import storm.util.ConfigUtils;
import storm.util.SqlUtils;

/**
 * @author wza
 * 预警规则获取
 */
public class EarlyWarnsGetter {

    private static final Logger LOG = LoggerFactory.getLogger(EarlyWarnsGetter.class);

    private static final SqlUtils SQL_UTILS = SqlUtils.getInstance();

    public static final String ALL = "ALL";

    /**
     * 平台报警规则查询 SQL
     */
    private static final String ALARM_RULE_SQL;

    static {
        final ConfigUtils configUtils = ConfigUtils.getInstance();
        final Properties sysParams = configUtils.sysParams;
        ALARM_RULE_SQL = sysParams.getProperty(
            "alarm.rule.sql",
            "select id,name,l1_seq_no,is_last1,l2_seq_no,is_last2,expr_left,r1_val,r2_val,expr_mid,levels,ifnull(veh_model_id,'ALL') from sys_data_const where is_valid=1 and type=1 and (depend_id is null or depend_id = '')");
        if(StringUtils.isBlank(ALARM_RULE_SQL)) {
            LOG.error("空白的平台报警规则查询语句.");
        } else {
            rebuild();
        }
    }

    private static ImmutableMap<String, ImmutableMap<String, EarlyWarn>> rules = ImmutableMap.of();

    public static void rebuild() {

        try {

            LOG.info("平台报警规则初始化开始.");

            rules = buildEarlyWarnFromDb();

            LOG.info("平台报警规则初始化完毕.");

        } catch (final Exception e) {
            LOG.warn("平台报警规则初始化异常", e);
        }
    }

    @NotNull
    private static ImmutableMap<String, ImmutableMap<String, EarlyWarn>> buildEarlyWarnFromDb() {
        return SQL_UTILS.query(ALARM_RULE_SQL, resultSet -> {

            final HashMap<String, EarlyWarn> items = Maps.newHashMapWithExpectedSize(100);

            while (resultSet.next()) {

                final String ruleId = resultSet.getString(1);
                if(StringUtils.isBlank(ruleId)) {
                    LOG.warn("空白的平台报警规则ID");
                }
                final String ruleName = resultSet.getString(2);
                final String left1DataKey = resultSet.getString(3);
                if(StringUtils.isBlank(left1DataKey)) {
                    LOG.warn("平台报警规则[{}][{}]空白的左一数据键", ruleId, ruleName);
                }
                final boolean left1UsePrev = StringUtils.equals("1", resultSet.getString(4));
                final String left2DataKey = resultSet.getString(5);
                final boolean left2UsePrev = StringUtils.equals("1", resultSet.getString(6));
                final String arithmeticExpression = resultSet.getString(7);
                final String right1Value = resultSet.getString(8);
                if(StringUtils.isBlank(right1Value)) {
                    LOG.warn("平台报警规则[{}][{}]空白的右一数据值", ruleId, ruleName);
                }
                final String right2Value = resultSet.getString(9);
                final String logicExpression = resultSet.getString(10);
                if(StringUtils.isBlank(logicExpression)) {
                    LOG.warn("平台报警规则[{}][{}]空白的逻辑运算符", ruleId, ruleName);
                }

                final BiFunction<ImmutableMap<String, String>, ImmutableMap<String, String>, Boolean> function =
                    EarlyWarn.buildFunction(
                        ruleId, ruleName,
                        left1DataKey, left1UsePrev, left2DataKey, left2UsePrev, arithmeticExpression,
                        right1Value, right2Value, logicExpression
                    );

                final int level = ((Function<String, Integer>)s->{
                    if(NumberUtils.isDigits(s)) {
                        return NumberUtils.toInt(s);
                    }
                    LOG.warn("平台报警规则[{}][{}]报警等级[{}]非数字", ruleId, ruleName, s);
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

                if(!items.containsKey(ruleId)) {
                    items.put(ruleId, earlyWarn);
                } else {
                    LOG.warn("重复的平台报警规则[{}][{}][{}]", ruleId, ruleName, items.get(ruleId).ruleName);
                }
            }

            return ImmutableMapUtils.group(items, (k, v) -> v.vehicleModelId);
        });
    }

    public static ImmutableMap<String, EarlyWarn> getRules(@Nullable final String vehicleModel) {

        return ((Function<ImmutableMap<String, ImmutableMap<String, EarlyWarn>>, ImmutableMap<String, EarlyWarn>>)
            (warns) ->{
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
        }).apply(rules);
    }
}
