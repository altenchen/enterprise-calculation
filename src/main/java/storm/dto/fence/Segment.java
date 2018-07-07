package storm.dto.fence;

import java.io.Serializable;

/**
 * @author wza
 * 电子围栏处理: 线段类
 */
public final class Segment implements Serializable {
    private static final long serialVersionUID = 1655500003L;

    /**
     * 起点
     */
    public Coordinate st;

    /**
     * 终点
     */
    public Coordinate ed;

    /**
     * 线段宽度
     */
    public double width;

    public Segment(Coordinate st, Coordinate ed, double width) {
        this.st = st;
        this.ed = ed;
        this.width = width;
    }
}
                                                  