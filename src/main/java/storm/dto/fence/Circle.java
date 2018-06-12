package storm.dto.fence;

import java.io.Serializable;

/**
 * @author xzp
 * 电子围栏处理: 圆形
 */
public final class Circle implements Serializable {
	private static final long serialVersionUID = 1655500007L;

    /**
     * 圆心
     */
	public final Coordinate center;

    /**
     * 半径
     */
	public final double radius;

	public Circle(Coordinate center, double radius) {
		this.center = center;
		this.radius = radius;
	}
}
                                                  