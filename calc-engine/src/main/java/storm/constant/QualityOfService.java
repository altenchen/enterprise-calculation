package storm.constant;

/**
 * 服务质量
 * @author xzp
 */
public final class QualityOfService {

    public static final String KEY = "QoS";

    /**
     * 最多一次
     */
    public static final byte AT_MOST_ONCE = 0;

    /**
     * 至少一次
     */
    public static final byte AT_LEAST_ONCE = 1;

    /**
     * 刚好一次
     */
    public static final byte EXACTLY_ONCE = 2;
}
