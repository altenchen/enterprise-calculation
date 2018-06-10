package storm.util;

import org.apache.kafka.common.utils.Utils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jgroups.annotations.Unsupported;

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

	@NotNull
	public static String deserialize(ByteBuffer buffer)
        throws UnsupportedEncodingException {

		String string;

        if (buffer.hasArray()) {
            final byte[] bytes = buffer.array();
            final int offset = buffer.arrayOffset() + buffer.position();
            final int length = buffer.remaining();
            string= new String(bytes, offset, length);
        } else {
            final byte[] bytes = Utils.toArray(buffer);
            // UTF-8 字符串
            string= new String(bytes, "UTF-8");
        }
        buffer.clear();

		return string;
	}
}
