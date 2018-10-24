package storm.handler.cusmade;

import storm.dto.FillChargeCar;
import storm.util.GpsUtil;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
/**
 * @author 于心沼
 * 查找附近补电车
 */
public class FindChargeCarsOfNearby {
    /**
     * 查找附近的补电车
     * @param longitude  经度
     * @param latitude   纬度
     * @param fillvidgps 缓存的补电车 vid [经度，纬度]
     * @return <距离,补电车>
     */
    public Map<Double, List<FillChargeCar>> findChargeCarsOfNearby(double longitude, double latitude, Map<String, FillChargeCar> fillvidgps) {
        if (null == fillvidgps || fillvidgps.size() == 0) {
            return null;
        }
        //检查经纬度是否为无效值
        boolean gpsIsInvalid = (0 == longitude && 0 == latitude) || Math.abs(longitude) > 180 || Math.abs(latitude) > 90;
        if (gpsIsInvalid) {
            return null;
        }
        //改用了list去存放，避免了当有距离相同的补电车时，会覆盖的问题
        Map<Double, List<FillChargeCar>> carSortMap = new TreeMap<>();

        for (Map.Entry<String, FillChargeCar> entry : fillvidgps.entrySet()) {
            FillChargeCar chargeCar = entry.getValue();
            double distance = GpsUtil.getDistance(longitude, latitude, chargeCar.longitude, chargeCar.latitude);
            if (carSortMap.containsKey(distance)){
                carSortMap.get(distance).add(chargeCar);
            }

            List<FillChargeCar> listOfChargeCar = new LinkedList<>();
            listOfChargeCar.add(chargeCar);
            carSortMap.put(distance, listOfChargeCar);
        }
        if (carSortMap.size() > 0) {
            return carSortMap;
        }
        return null;
    }

}
