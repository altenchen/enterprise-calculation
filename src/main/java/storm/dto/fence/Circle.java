package storm.dto.fence;

import java.io.Serializable;

public class Circle implements Serializable {

	/**
	 * 圆类
	 */
	private static final long serialVersionUID = 1655500007L;

	public Coordinate center;//圆心
	public double radius=0;//半径
	public Circle(Coordinate center, double radius) {
		super();
		this.center = center;
		this.radius = radius;
	}
	
}
                                                  