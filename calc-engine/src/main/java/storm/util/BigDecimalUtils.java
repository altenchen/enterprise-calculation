package storm.util;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author: xzp
 * @date: 2018-09-21
 * @description:
 */
public final class BigDecimalUtils {

    @Contract("null -> null")
    @Nullable
    public static BigDecimal stripTrailingZeros(@Nullable BigDecimal value) {
        if(null == value) {
            return null;
        }
        return value.stripTrailingZeros();
    }

    @Contract("null, _ -> null")
    @Nullable
    public static BigDecimal scale(@Nullable BigDecimal value, int scale) {
        if(null == value) {
            return null;
        }
        return value.setScale(scale, RoundingMode.HALF_UP);
    }

    @Contract("null, _, _ -> null")
    @Nullable
    public static BigDecimal scale(@Nullable BigDecimal value, int scale, RoundingMode roundingMode) {
        if(null == value) {
            return null;
        }
        return value.setScale(scale, roundingMode);
    }
}
