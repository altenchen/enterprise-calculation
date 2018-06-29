package storm.handler.cusmade;

import com.sun.jersey.core.util.Base64;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Nullable;
import storm.system.DataKey;

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 判断无CAN的默认实现
 * @author: xzp
 * @date: 2018-06-15
 * @description:
 */
public class CarNoCanDecideDefault implements ICarNoCanDecide {

    @Override
    public boolean hasCan(Map<String, String> data) {

        // 车辆状态
        String carStatus = data.get(DataKey._3201_CAR_STATUS);
        // 车辆SOC
        String soc = data.get(DataKey._7615_STATE_OF_CHARGE);
        if(!StringUtils.isBlank(carStatus)
            && !StringUtils.isBlank(soc)) {
            return true;
        }

        // 电机CAN
        String macList = data.get(DataKey._2308_DRIVING_ELE_MAC_LIST);
        if(hasMacCan(macList)) {
            return true;
        }

        // 电池单体电压最高值
        String hignVolt = data.get(DataKey._2603_SINGLE_VOLT_HIGN_VAL);
        // 电池单体最低温度值
        String lowTemp = data.get(DataKey._2612_SINGLE_LOWTEMP_VAL);
        if(!StringUtils.isBlank(hignVolt)
            && !StringUtils.isBlank(lowTemp)) {
            return true;
        }

        // CAN列表, 定制协议
        String canList = data.get(DataKey._4410023_CAN_LIST);
        if(!StringUtils.isBlank(canList)) {
            return true;
        }

        return false;
    }

    private static final Set<String> drivingMotorMacKey = new HashSet();
    static {
        //"2302";//driving 驱动电机温度控制器
        drivingMotorMacKey.add(DataKey._2302_DRIVING_ELE_MAC_TEMPCTOL);
        //"2303";//driving 驱动电机转速
        drivingMotorMacKey.add(DataKey._2303_DRIVING_ELE_MAC_REV);
        //"2304";//driving 驱动电机温度
        drivingMotorMacKey.add(DataKey._2304_DRIVING_ELE_MAC_TEMP);
        //"2305";//driving 驱动电机输入电压
        drivingMotorMacKey.add(DataKey._2305_DRIVING_ELE_MAC_VOLT);
        //"2306";//driving 驱动电机母线电流
        drivingMotorMacKey.add(DataKey._2306_DRIVING_ELE_MAC_ELE);
        //"2310";//driving 驱动电机状态
        drivingMotorMacKey.add(DataKey._2310_DRIVING_ELE_MAC_STATUS);
        //"2311";//driving 驱动电机转矩
        drivingMotorMacKey.add(DataKey._2311_DRIVING_ELE_MAC_TORQUE);
    }
    /**
     * 判断是否有电机CAN
     * @param macList
     * @return
     */
    private static final boolean hasMacCan(@Nullable String macList) {
        if (StringUtils.isBlank(macList)) {
            return false;
        }

        String[] drivingMotors = macList.split("\\|");
        if (drivingMotors == null || drivingMotors.length <= 0) {
            return false;
        }

        for (String drivingMotor : drivingMotors) {
            final String value;
            try {
                value = new String(Base64.decode(drivingMotor), "GBK");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                continue;
            }

            final String[] params = value.split(",");

            for (String param : params) {
                final String[] p = param.split(":", 2);
                if (p != null && p.length == 2) {
                    final String macKey = p[0];
                    final String macValue = p[1];
                    // 只要有其中之一的数据有效就说明有电机can状态
                    if(drivingMotorMacKey.contains(macKey)
                        && !StringUtils.isBlank(macValue)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
