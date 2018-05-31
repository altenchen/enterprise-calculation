package storm.dto.fence;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class PolygonCal implements Serializable {
	/**
	 * 多边形
	 */
	private static final long serialVersionUID = 1655500006L;
	
	private List<Segment>segments;//一系列线段组成
	//segments 与 coords只需具备其中之一即可
	public List<Coordinate> coords;//按照顺序需要画的点

	public PolygonCal() {
		super();
	}
	public PolygonCal(List<Coordinate> coords) {
		super();
		this.coords = coords;
	}

	public List<Segment> getSegments() {
		return segments;
	}

	public void setSegments(List<Segment> segments) {
		this.segments = segments;
	}
	public void addCoordinate(Coordinate c){
		if (null == coords) 
			coords = new LinkedList<Coordinate>();
		if (null != c) 
			coords.add(c);
	}

}
