package storm.dto;

import java.io.Serializable;

/**
 * 补电车
 *
 * @author xzp
 */
public class FillChargeCar implements Serializable {

    private static final long serialVersionUID = 29000006L;

    /**
     * 车辆唯一识别码
     */
    public final String vid;

    /**
     * 经度
     */
    public final double longitude;

    /**
     * 纬度
     */
    public final double latitude;

    /**
     * 最后在线时间
     *
     * yyyyMMddhhmmss
     */
    public final String lastOnline;

    /**
     *
     */
    public final int running;

    public FillChargeCar(
        final String vid,
        final double longitude,
        final double latitude,
        final String lastOnline) {

        this(vid, longitude, latitude, lastOnline, 0);
    }

    public FillChargeCar(
        final String vid,
        final double longitude,
        final double latitude,
        final String lastOnline,
        final int running) {

        this.vid = vid;
        this.longitude = longitude;
        this.latitude = latitude;
        this.lastOnline = lastOnline;
        this.running = running;
    }

}
