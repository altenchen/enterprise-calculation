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

    /**
     * 比较两个坐标点是否相等
     * @param obj
     * @return
     */
    @Override
    public boolean equals(final Object obj) {
        if( this == obj ){
            return true;
        }
        Coordinate target = (Coordinate) obj;
        if( Double.valueOf(this.longitude).equals(Double.valueOf(target.longitude)) && Double.valueOf(this.latitude).equals(Double.valueOf(target.latitude)) ){
            return true;
        }
        return false;
    }
}
                                                  