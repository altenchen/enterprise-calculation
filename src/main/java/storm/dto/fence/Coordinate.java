package storm.dto.fence;

import java.io.Serializable;

/**
 * @author xzp
 * 电子围栏处理: 坐标
 */
public final class Coordinate implements Serializable {
	private static final long serialVersionUID = 1655500001L;

    /**
     * 经度
     */
	public final double x;

    /**
     * 纬度
     */
	public final double y;

	public Coordinate(double x, double y) {
		this.x = x;
		this.y = y;
	}
}
                                                  