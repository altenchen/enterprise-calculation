package storm.domain.fence.service;

import storm.domain.fence.Fence;

import java.util.stream.Stream;

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
    Stream<Fence> query(String vid);

}
