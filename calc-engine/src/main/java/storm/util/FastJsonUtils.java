package storm.util;

import org.jetbrains.annotations.Contract;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-07-12
 * @description:
 */
public final class FastJsonUtils {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(FastJsonUtils.class);

    private static final FastJsonUtils INSTANCE = new FastJsonUtils();

    @Contract(pure = true)
    public static FastJsonUtils getInstance() {
        return INSTANCE;
    }

    private FastJsonUtils() {
        if (INSTANCE != null) {
            throw new IllegalStateException();
        }
    }
}
