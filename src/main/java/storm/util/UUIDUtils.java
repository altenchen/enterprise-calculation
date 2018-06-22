package storm.util;

import java.util.UUID;

/**
 * @author xzp
 */
public class UUIDUtils {

	public static String getUUIDString(){
        return UUID
            .randomUUID()
            .toString()
            .replaceAll("-", "");
	}
}
