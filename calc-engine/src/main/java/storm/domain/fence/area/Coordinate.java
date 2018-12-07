package storm.domain.fence.area;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * 坐标
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
public final class Coordinate {

    /**
     * 经度
     */
    public final double longitude;

    /**
     * 纬度
     */
    public final double latitude;

    public Coordinate(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @NotNull
    @Contract(pure = true)
    @Override
    public String toString() {
        return "(" + longitude + "," + latitude + ")";
    }
}
                                                  