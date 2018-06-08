package storm.util;

import org.apache.kafka.common.utils.Utils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

public class ObjectUtils {

    @Contract("null -> true")
    public static boolean isNullOrWhiteSpace(String string){
        if(null == string || "".equals(string))
            return true;
        return "".equals(string.trim());
    }

	@Contract(value = "null -> true", pure = true)
    public static boolean isNullOrEmpty(String string){
		if(null == string || "".equals(string))
			return true;
		return false;
	}

	@Contract("null -> true")
    public static boolean isNullOrEmpty(Collection collection){
		if (null == collection || collection.isEmpty())
			return true;
		return false;
	}

	@Contract("null -> true")
    public static boolean isNullOrEmpty(Map map){
		if(map == null || map.isEmpty())
			return true;
		return false;
	}

	@Contract("null -> true")
    public static boolean isNullOrEmpty(Object object){
		if(null == object)
			return true;
		if (object instanceof String) 
			return isNullOrEmpty((String)object);
		else if (object instanceof Map) 
			return isNullOrEmpty((Map)object);
		else if (object instanceof Collection) 
			return isNullOrEmpty((Collection)object);
		
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
