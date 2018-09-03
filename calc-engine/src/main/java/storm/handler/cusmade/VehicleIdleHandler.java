package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-09-03
 * @description:
 * 车辆闲置处理, 用于判断车辆是否闲置.
 */
public final class VehicleIdleHandler implements Serializable {

    private static final long serialVersionUID = -7534407274883592332L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(VehicleIdleHandler.class);

    /**
     * 车辆实时数据缓存
     */
    private final Map<String, Object> vehicleRealtimeDataCache = Maps.newHashMap();

    /**
     * 车辆闲置通知缓存
     */
    private final Map<String, Object> vehicleIdleNoticeCache = Maps.newHashMap();

    public void processRealtimeData(@NotNull final ImmutableMap<String, String> realtimeData) {

    }
}
