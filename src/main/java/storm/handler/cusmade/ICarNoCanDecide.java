package storm.handler.cusmade;

import java.util.Map;

/**
 * 车辆无CAN判定条件
 * @author: xzp
 * @date: 2018-06-15
 * @description:
 */
public interface ICarNoCanDecide {

    /**
     * 判断车辆是否有CAN
     * @param data 车辆数据
     * @return 车辆是否有CAN
     */
    boolean hasCan(Map<String, String> data);
}
