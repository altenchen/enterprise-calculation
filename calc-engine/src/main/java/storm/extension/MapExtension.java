package storm.extension;

import org.apache.commons.collections.MapUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-10-16
 * @description:
 */
public final class MapExtension {

    public static <K, V> void clearNullEntry(@Nullable final Map<K, V> map) {
        if (MapUtils.isNotEmpty(map)) {
            map.remove(null);
            map.entrySet().removeIf(entry -> null == entry.getValue());
        }
    }
}
