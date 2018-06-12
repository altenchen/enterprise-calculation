package storm.dto.fence;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * @author wza
 * 电子围栏处理: 多边形
 */
public final class PolygonCal implements Serializable {
	private static final long serialVersionUID = 1655500006L;

    /**
     * 按照顺序需要画的点
     */
	public final List<Coordinate> coords = new LinkedList<>();

    public void addCoordinate(@NotNull Coordinate c) {
        coords.add(c);
    }

}
