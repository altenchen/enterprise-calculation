package storm.handler.cusmade;

import org.apache.commons.lang.StringUtils;
import storm.system.DataKey;
import storm.util.ObjectUtils;

import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-06-15
 * @description:
 */
public class CarNoCanDecideJili implements ICarNoCanDecide {

    /**
     * 吉利报表是否有CAN判定, {车速, 累计里程, 总电压, 总电流, SOC} 只要其中一个值有效, 则判定为有CAN, 否则判定为无CAN.
     * @param data 实时数据
     * @return 车辆是否有CAN
     */
    @Override
    public boolean hasCan(Map<String, String> data) {

        // region 车速
        {
            final String speedString = data.get(DataKey._2201_SPEED);
            if (!StringUtils.isBlank(speedString)) {
                return true;
            }
        }
        // endregion

        // region 累计里程
        {
            final String totalMileageString = data.get(DataKey._2202_TOTAL_MILEAGE);
            if (!StringUtils.isBlank(totalMileageString)) {
                return true;
            }
        }
        // endregion

        // region 总电压
        {
            final String totalVoltageString = data.get(DataKey._2613_TOTAL_VOLTAGE);
            if (!StringUtils.isBlank(totalVoltageString)) {
                return true;
            }
        }
        // endregion

        // region 总电流
        {
            final String totalElectricityString = data.get(DataKey._2614_TOTAL_ELECTRICITY);
            if (!StringUtils.isBlank(totalElectricityString)) {
                return true;
            }
        }
        // endregion

        // region SOC
        {
            final String stateOfChargeString = data.get(DataKey._7615_STATE_OF_CHARGE);
            if (!StringUtils.isBlank(stateOfChargeString)) {
                return true;
            }
        }
        // endregion

        return false;
    }
}
