package storm.domain.fence.service;

import com.google.common.collect.ImmutableMap;
import storm.dao.DataToRedis;
import storm.domain.fence.Fence;

import java.util.List;
import java.util.function.Consumer;

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
     * @return
     */
    boolean existFence(String fenceId);

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
     * 数据检查,删除redis上的脏数据
     *
     * @param redis
     */
    void dataCheck(DataToRedis redis);

}
