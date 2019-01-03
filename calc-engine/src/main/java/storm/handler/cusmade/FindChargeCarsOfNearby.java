package storm.handler.cusmade;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.jetbrains.annotations.Nullable;
import storm.dto.FillChargeCar;
import storm.util.GpsUtil;

import java.util.List;
import java.util.Map;
/**
 * @author 于心沼
 * 查找附近补电车
 */
public final class FindChargeCarsOfNearby {

    /**
     * 查找附近的补电车
     * @param longitude  经度
     * @param latitude   纬度
     * @param fillVehicleGpsCache 缓存的补电车 <vid, [经度，纬度]>
     * @return <距离,补电车>
     */
    @Nullable
    public static Map<Double, List<FillChargeCar>> findChargeCarsOfNearby(
        final double longitude,
        final double latitude,
        @Nullable final Map<String, FillChargeCar> fillVehicleGpsCache) {
        if(MapUtils.isEmpty(fillVehicleGpsCache)) {
            return null;
        }

        //检查经纬度是否为无效值
        final double absLongitude = Math.abs(longitude);
        final double absLatitude = Math.abs(latitude);
        if (0 == absLongitude || absLongitude > 180 || 0 == absLatitude || absLatitude > 90) {
            return null;
        }

        //改用了list去存放，避免了当有距离相同的补电车时，会覆盖的问题
        final Map<Double, List<FillChargeCar>> carSortMap = Maps.newTreeMap();
        for (final Map.Entry<String, FillChargeCar> entry : fillVehicleGpsCache.entrySet()) {
            final FillChargeCar chargeCar = entry.getValue();
            final double distance = GpsUtil.getDistance(longitude, latitude, chargeCar.longitude, chargeCar.latitude);
            carSortMap.compute(
                distance,
                (final Double key, final List<FillChargeCar> oldValue) ->{
                    final List<FillChargeCar> newValue = null == oldValue ? Lists.newLinkedList() : oldValue;
                    newValue.add(chargeCar);
                    return newValue;
                });
        }

        if (MapUtils.isNotEmpty(carSortMap)) {
            return carSortMap;
        }
        return null;
    }

}
