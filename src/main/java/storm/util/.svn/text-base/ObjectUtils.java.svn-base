package storm.util;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

public class ObjectUtils {
	public static boolean isNullOrEmpty(String string){
		if(null == string || "".equals(string))
			return true;
		return "".equals(string.trim());
	}
	public static boolean isNullOrEmpty(Collection collection){
		if (null == collection || collection.size()==0) 
			return true;
		return false;
	}
	public static boolean isNullOrEmpty(Map map){
		if(map == null || map.size()==0)
			return true;
		return false;
	}
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

	public static String deserialize(ByteBuffer buffer){
		String string=null;
		try {
			if (buffer.hasArray()) {
			    int base = buffer.arrayOffset();
			    string= new String(buffer.array(), base + buffer.position(), buffer.remaining());
			} else {
				string= new String(org.apache.kafka.common.utils.Utils.toArray(buffer), "UTF-8");
			}
			buffer.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return string;
	}
}
