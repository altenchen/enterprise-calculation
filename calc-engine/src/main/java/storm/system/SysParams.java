package storm.system;

import org.jetbrains.annotations.Contract;
import storm.util.ConfigUtils;

import java.util.Properties;

/**
 * @author: xzp
 * @date: 2018-06-21
 * @description: 未来版本删除
 */
@Deprecated
public final class SysParams {

    public static final String CHARGE_CAR_TYPE_ID = "charge.car.type.id";

    public static final String ALARM_CODE_SQL = "alarm.code.sql";

    public static final String ALARM_CODE_BIT_SQL = "alarm.code.bit.sql";

    public static final String VEH_MODEL_SQL = "veh.model.sql";

    public static final String EARLY_WARNING_SQL = "early.warning.sql";

    public static final String ITEM_COEF_OFFSET_SQL = "item.coef.offset.sql";

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
}
