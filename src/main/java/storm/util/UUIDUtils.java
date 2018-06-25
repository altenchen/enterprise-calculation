package storm.util;

import java.util.UUID;

/**
 * @author unknown
 */
public class UUIDUtils {

	public static String getUUIDString(){
        return UUID
            .randomUUID()
            .toString()
            .replaceAll("-", "");
	}
}
