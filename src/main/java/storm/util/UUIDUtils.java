package storm.util;

import java.util.UUID;

/**
 * @author unknown
 */
public class UUIDUtils {

	public static String randomUuidString(){
		String uuid = UUID.randomUUID().toString();
		uuid = uuid.replace("-", "");
		return uuid;
	}
}
