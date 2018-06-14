package storm.handler.cusmade;

import storm.system.DataKey;
import storm.util.ObjectUtils;

import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-06-15
 * @description:
 */
public class CarNoCanDecideJili implements ICarNoCanDecide {
    // TODO XZP: Redis取值覆盖
    // region 吉利判定参数
    private static final short MIN_2201_SPEED_JILI = 0;
    private static final short MAX_2201_SPEED_JILI = 200 * 10;
    private static final short MIN_2613_TOTAL_VOLTAGE_JILI = 0;
    private static final short MAX_2613_TOTAL_VOLTAGE_JILI = 1000 * 10;
    private static final byte MIN_7615_STATE_OF_CHARGE_JILI = 0;
    private static final byte MAX_7615_STATE_OF_CHARGE_JILI = 100;
    private static final byte EQUALS_2501_ORIENTATION_JILI = 0;
    private static final int MIN_2502_LONGITUDE_JILI = 73;
    private static final int MAX_2502_LONGITUDE_JILI = 135;
    private static final int MIN_2503_LATITUDE_JILI = 4;
    private static final int MAX_2503_LATITUDE_JILI = 53;
    // endregion

    /**
     * 吉利报表是否有CAN判定
     * @param data
     * @return
     */
    @Override
    public boolean hasCan(Map<String, String> data) {

        // region 车速 ∈ [0, 200)
        {
            final String speedString = data.get(DataKey._2201_SPEED);
            if (!ObjectUtils.isNullOrWhiteSpace(speedString)) {
                try {
                    final short speed = Short.parseShort(speedString);
                    if (speed >= MIN_2201_SPEED_JILI && speed < MAX_2201_SPEED_JILI) {
                        return true;
                    }
                } catch (NumberFormatException ignored) {
                    // nextJudge
                }
            }
        }
        // endregion

        // TODO XZP: [待确认] 上线开始里程, 上线结束里程
        // region 累计里程 ∈ [上线开始里程, 上线结束里程]
        {
            final String totalMileageString = data.get(DataKey._2202_TOTAL_MILEAGE);
            if (!ObjectUtils.isNullOrWhiteSpace(totalMileageString)) {
                try {
                    final int totalMileage = Integer.parseUnsignedInt(totalMileageString);
                    if (totalMileage >= DataKey.MIN_2202_TOTAL_MILEAGE && totalMileage <= DataKey.MAX_2202_TOTAL_MILEAGE) {
                        return true;
                    }
                } catch (NumberFormatException ignored) {
                    // nextJudge
                }
            }
        }
        // endregion

        // region 总电压 ∈ (0, 1000]
        {
            final String totalVoltageString = data.get(DataKey._2613_TOTAL_VOLTAGE);
            if (!ObjectUtils.isNullOrWhiteSpace(totalVoltageString)) {
                try {
                    final short totalVoltage = Short.parseShort(totalVoltageString);
                    if (totalVoltage > MIN_2613_TOTAL_VOLTAGE_JILI && totalVoltage <= MAX_2613_TOTAL_VOLTAGE_JILI) {
                        return true;
                    }
                } catch (NumberFormatException ignored) {
                    // nextJudge
                }
            }
        }
        // endregion

        // TODO XZP: [有时间设计算法, 没时间就用国标范围]一段时间内总电流均值, 目前算法未定.
        // region 总电流 ∈ [总电流均值 - 100, 总电流均值 + 100]
        {
            final String totalElectricityString = data.get(DataKey._2614_TOTAL_ELECTRICITY);
            if (!ObjectUtils.isNullOrWhiteSpace(totalElectricityString)) {
                try {
                    final short totalElectricity = Short.parseShort(totalElectricityString);
                    if (totalElectricity >= DataKey.MIN_2614_TOTAL_ELECTRICITY
                        && totalElectricity <= DataKey.MAX_2614_TOTAL_ELECTRICITY) {
                        return true;
                    }
                } catch (NumberFormatException ignored) {
                    // nextJudge
                }
            }
        }
        // endregion

        // region SOC ∈ (0, 100]
        {
            final String stateOfChargeString = data.get(DataKey._7615_STATE_OF_CHARGE);
            if (!ObjectUtils.isNullOrWhiteSpace(stateOfChargeString)) {
                try {
                    final byte stateOfCharge = Byte.parseByte(stateOfChargeString);
                    if (stateOfCharge >= MIN_7615_STATE_OF_CHARGE_JILI
                        && stateOfCharge <= MAX_7615_STATE_OF_CHARGE_JILI) {
                        return true;
                    }
                } catch (NumberFormatException ignored) {
                    // nextJudge
                }
            }
        }
        // endregion

        // region 定位状态 ∈ [0], 经度 ∈ (73, 135), 纬度 ∈ (4, 53)
        {
            final String orientationString = data.get(DataKey._2501_ORIENTATION);
            final String longitudeString = data.get(DataKey._2502_LONGITUDE);
            final String latitudeString = data.get(DataKey._2503_LATITUDE);
            if (!ObjectUtils.isNullOrWhiteSpace(orientationString)
                && !ObjectUtils.isNullOrWhiteSpace(longitudeString)
                && !ObjectUtils.isNullOrWhiteSpace(latitudeString)) {
                try {
                    final int orientation = Integer.parseUnsignedInt(orientationString);
                    final int longitude = Integer.parseUnsignedInt(longitudeString);
                    final int latitude = Integer.parseUnsignedInt(latitudeString);
                    if (orientation == EQUALS_2501_ORIENTATION_JILI
                        && longitude >= MIN_2502_LONGITUDE_JILI
                        && longitude <= MAX_2502_LONGITUDE_JILI
                        && latitude >= MIN_2503_LATITUDE_JILI
                        && latitude <= MAX_2503_LATITUDE_JILI) {
                        return true;
                    }
                } catch (NumberFormatException ignored) {
                    // nextJudge
                }
            }
        }
        // endregion

        return false;
    }
}
