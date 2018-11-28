package storm.dto.fence;

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
}
                                                  