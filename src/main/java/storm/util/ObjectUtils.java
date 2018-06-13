package storm.util;

import org.apache.kafka.common.utils.Utils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jgroups.annotations.Unsupported;
import storm.system.DataKey;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * @author xzp
 */
@SuppressWarnings("unused")
public class ObjectUtils {

    /**
     * @param string 字符串
     * @return 是否空白字符串
     */
    @Contract("null -> true")
    public static boolean isNullOrWhiteSpace(String string){
        return null == string || string.chars().noneMatch(c -> c != ' ');
    }

    /**
     * @param string 字符串
     * @return 是否空字符串
     */
	@Contract(value = "null -> true", pure = true)
    public static boolean isNullOrEmpty(String string){
		return null == string || "".equals(string);
	}

    /**
     * @param collection 集合
     * @return 是否空集合
     */
	@Contract("null -> true")
    public static boolean isNullOrEmpty(Collection collection){
		return null == collection || collection.isEmpty();
	}

    /**
     * @param map 字典
     * @return 是否空字典
     */
	@Contract("null -> true")
    public static boolean isNullOrEmpty(Map map){
		return map == null || map.isEmpty();
	}

    /**
     * 对象转字符串
     * @param obj
     * @return 如果对象为null则返回null, 否则返回对象的字符串表示方式
     */
    @Nullable
    @Contract("null -> null")
    public static String getNullOrString(@Nullable Object obj) {
        return null == obj ? null : obj.toString();
    }

    /**
     * 从字典中提取数据项的值的字符串表示方式
     * @param map 存放数据的字典
     * @param key 数据项的键
     * @return 如果数据项的值为null则返回null, 否则返回数据项的值的字符串表示方式
     */
    @Nullable
    public static String getNullOrString(@NotNull Map map, @NotNull Object key) {
        return getNullOrString(map.get(key));
    }

    /**
     * 该方法判断是否一个空集合, 如果不是集合, 就转成字符串处理, 不是太推荐使用这个不明确的方法.
     * @param object 对象
     * @return 是否空对象
     */
    @Deprecated
	@Contract("null -> true")
    public static boolean isNullOrEmpty(Object object){
		if(null == object) {
			return true;
		}
		if (object instanceof String) {
			return isNullOrEmpty((String)object);
		} else if (object instanceof Map) {
			return isNullOrEmpty((Map)object);
		} else if (object instanceof Collection) {
			return isNullOrEmpty((Collection)object);
		}
		
		return isNullOrEmpty(object.toString());
	}

    /**
     * 将字节缓冲区中的数据转成字符串
     * @param buffer
     * @return
     * @throws UnsupportedEncodingException
     */
	@NotNull
	public static String deserialize(ByteBuffer buffer)
        throws UnsupportedEncodingException {

		String string;

		// region 这段逻辑在Utils.toArray内部也有, 疑似误用, 有待验证.
        if (buffer.hasArray()) {
            final byte[] bytes = buffer.array();
            final int offset = buffer.arrayOffset() + buffer.position();
            final int length = buffer.remaining();
            string= new String(bytes, offset, length);
        }
        // endregion
        else {
            final byte[] bytes = Utils.toArray(buffer);
            // UTF-8 字符串
            string= new String(bytes, "UTF-8");
        }
        buffer.clear();

		return string;
	}


    /**
     * 判断报文是否为自动唤醒报文,判断依据：总电压、总电流同时为空则为自动唤醒数据
     * @param map 集合
     * @return 是否为自动唤醒报文
     */
    @Contract("null -> true")
    public static boolean JudgeAutoWake(Map map){
        if(null == map.get(DataKey._2613_TOTAL_VOLTAGE)
                || "".equals(map.get(DataKey._2613_TOTAL_VOLTAGE))
                && null == map.get(DataKey._2614_TOTAL_ELECTRICITY)
                || "".equals(map.get(DataKey._2613_TOTAL_VOLTAGE))) {
            return true;
        }
    }

}
