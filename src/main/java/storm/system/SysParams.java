package storm.system;

import org.jetbrains.annotations.Contract;
import storm.util.ConfigUtils;

import java.util.Properties;

/**
 * @author: xzp
 * @date: 2018-06-21
 * @description:
 */
public final class SysParams {

    private static final ConfigUtils configUtils = ConfigUtils.getInstance();
    private static final Properties sysParams = configUtils.sysParams;

    public static final String CHARGE_CAR_TYPE_ID = "charge.car.type.id";

    public static final String ALARM_CODE_SQL = "alarm.code.sql";

    public static final String ALARM_CODE_BIT_SQL = "alarm.code.bit.sql";

    public static final String VEH_MODEL_SQL = "veh.model.sql";

    public static final String FENCE_SQL = "fence.sql";

    public static final String EARLY_WARNING_SQL = "early.warning.sql";

    public static final String ITEM_COEF_OFFSET_SQL = "item.coef.offset.sql";


    public String getProperty(String property) {
        return sysParams.getProperty(property);
    }

    public String getProperty(String property, String defaultValue) {
        return sysParams.getProperty(property, defaultValue);
    }

    private static final SysParams INSTANCE = new SysParams();

    @Contract(pure = true)
    public static SysParams getInstance() {
        return INSTANCE;
    }

    private SysParams() {
        if(INSTANCE != null) {
            throw new IllegalStateException();
        }
    }

    /**
     * @return 获取充电车ID
     */
    public final String getChargeCarTypeId() {
        return getProperty(CHARGE_CAR_TYPE_ID);
    }

    /**
     * @return 从数据库获取故障码规则的SQL
     */
    public final String getAlarmCodeSql() {
        return getProperty(ALARM_CODE_SQL);
    }

    /**
     * @return 从数据库获取故障码按位解析规则的SQL
     */
    public final String getAlarmCodeBitSql() {
        return getProperty(ALARM_CODE_BIT_SQL);
    }

    /**
     * @return 从数据库获取电子围栏的SQL
     */
    public final String getFenceSql() {
        return getProperty(FENCE_SQL);
    }

    /**
     * @return 从数据库获取预警规则的SQL
     */
    public final String getEarlyWarningSql() {
        return getProperty(EARLY_WARNING_SQL);
    }

    public final String getItemCoefOffsetSql() {
        return getProperty(ITEM_COEF_OFFSET_SQL);
    }
}
