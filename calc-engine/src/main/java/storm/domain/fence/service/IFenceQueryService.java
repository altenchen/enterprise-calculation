package storm.domain.fence.service;

import com.google.common.collect.ImmutableMap;
import storm.domain.fence.Fence;

import java.util.function.BiConsumer;

/**
 * 查询电子围栏接口
 *
 * @author 智杰
 */
public interface IFenceQueryService {

    /**
     * 根据车辆VID查询出对应的电子围栏列表
     *
     * @param vid 车辆ID
     * @return 电子围栏列表
     */
    ImmutableMap<String, Fence> query(String vid);

    /**
     * 判断是否存在电子围栏
     *
     * @param fenceId 围栏ID
     * @param time    当前时间
     * @return
     */
    boolean existFence(String fenceId, long time);

    /**
     * 判断电子围栏与规则是否关联
     *
     * @param fenceId 围栏ID
     * @param eventId 规则ID
     * @return
     */
    boolean existFenceEvent(String fenceId, String eventId);

    /**
     * 判断是否存在有效的电子围栏与车辆关联
     *
     * @param fenceId   围栏ID
     * @param vehicleId 车辆ID
     * @return
     */
    boolean existFenceVehicle(String fenceId, String vehicleId);

    /**
     * 删除围栏与车辆驶入驶出状态缓存
     *
     * @param fenceId
     * @param vid
     */
    void deleteFenceVehicleStatusCache(String fenceId, String vid);

    /**
     * 将之前触发驶入驶出通知的车辆因为修改了围栏导致没有结束掉的缓存清理掉
     * @param consumer
     */
    void dirtyDataClear(BiConsumer<String, String> consumer);

}
