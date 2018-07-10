package storm.handler.area;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import storm.dto.area.AreaFence;
import storm.dto.fence.Coordinate;
import storm.util.FileSource;

public class AreaFenceHandler {

    List<AreaFence> areaFences;
    Map<String,AreaFence> areaIdFenceCache;
    void build(){
        //areafences为区域围栏，为areaFence的列表，areaFence包括区域id、区域名字、一个矩形（区域的最大最小经度，最大最小纬度四个值）。
        areaFences =FileSource.areaFences;
        //areaIdFenceCache中为（区域id，areafence对象）列表
        areaIdFenceCache =FileSource.areaIdFenceCache;
    }
    {
        build();
    }

    /**
     *
     * @param x 经度
     * @param y 纬度
     * @return 行政区域 id集合
     */
    public List<String> areaIds(double x, double y){
        if (0 <= x && x <= 180
                && 0 <= y && y <= 180) {
            return areaIds(new Coordinate(x, y));
        }
        return null;
    }

    /**
     * 获取 gps 点属于那个行政区域
     * @param coord gps经纬度点
     * @return 返回行政区域 id 的集合；所属地区 市区 省份
     */
    public List<String> areaIds(Coordinate coord){
        //indexs()获取 区域围栏的索引，用于缩小搜索范围
        Set<Integer> idxs = indexs(coord);
        if (null != idxs) {
            //ids用来盛放点所在的区域id
            List<String> ids = new LinkedList<String>();
            for (int idx : idxs) {
                AreaFence fence = areaFences.get(idx);

                if (!ids.contains(fence.areaId)) {

                    boolean inFence = isPointInPolygon(fence.index.coords, coord);
                    if (inFence) {

                        ids.add(fence.areaId);
                        if (null != fence.parentAreaId
                                && !ids.contains(fence.parentAreaId)) {

                            ids.add(fence.parentAreaId);
                        }
                    }
                } else if (null != fence.parentAreaId
                        && !ids.contains(fence.parentAreaId)) {
                    ids.add(fence.parentAreaId);
                }
            }
            int len = ids.size();
            if (len >0) {
                List<String> reIds = new ArrayList<String>(len);
                reIds.add("");
                if (len > 1) {
                    reIds.add("");
                }
                for (String id : ids) {
                    if (id.endsWith("0000")) {
                        reIds.set(0, id);
                    } else if (id.endsWith("00")
                            || 2 == len) {
                        reIds.set(1, id);
                    } else {
                        reIds.add(id);
                    }
                }
                ids = null;
                return reIds;
            }
        }
        return null;
    }

    /**
     * 获取 区域围栏的索引，用于缩小搜索范围
     * @param coord
     * @return 若干个矩形的index
     */
    private Set<Integer> indexs(Coordinate coord){
        if (null == areaFences || areaFences.size() ==0)
            return null;

        int len = areaFences.size();//总区域数
        int st = 0;
        int ed = len-1;
        int idx= (st+ed)/2;

        //高效二分法查找
        while(st < ed){

            AreaFence fence = areaFences.get(idx);

            if (coord.x < fence.index.xmin ) {
                ed = idx - 1;
            } else if (coord.x > fence.index.xmin) {
                st = idx + 1;
            } else {
                break;
            }
            idx= (st+ed)/2;
        }

        Set<Integer> ids = new HashSet<Integer>();
        for(int pos = 0; pos < idx; pos++){
            AreaFence fence = areaFences.get(pos);
            //0到idx-1的xmin都小于idx的coord.x。
            if (coord.x <= fence.index.xmax
                    && coord.y <= fence.index.ymax
                    && coord.y >= fence.index.ymin) {

                if (!ids.contains(pos)) {
                    ids.add(pos);
                }
            }
        }
        //因为上面可能会漏掉idx。所以下面对它进行判断
        for(int pos = idx; pos <= idx+1; pos++){
            if (pos < len) {

                AreaFence fence = areaFences.get(pos);
                if (coord.x <= fence.index.xmax
                        && coord.x >= fence.index.xmin
                        && coord.y <= fence.index.ymax
                        && coord.y >= fence.index.ymin) {

                    if (!ids.contains(pos)) {
                        ids.add(pos);
                    }
                }
            }
        }
        if (ids.size() > 0) {
            return ids;
        }
        return null;
    }

    /**
     * 判断 点是否在多边形的内部
     * @param coords
     * @param coord
     * @return 是或否
     */
    private boolean isPointInPolygon(List<Coordinate> coords, Coordinate coord) {  
        int i, j;  
        boolean c = false;  
        for (i = 0, j = coords.size() - 1; i < coords.size(); j = i++) {  
            //如果满足条件，则说明在多边形内
            if ((((coords.get(i).x <= coord.x) && (coord.x < coords  
                    .get(j).x)) || ((coords.get(j).x <= coord.x) && (coord.x < coords  
                    .get(i).x)))  
                    && (coord.y < (coords.get(j).y - coords.get(i).y)  
                            * (coord.x - coords.get(i).x)  
                            / (coords.get(j).x - coords.get(i).x)  
                            + coords.get(i).y)) {  
                c = !c;  
            }  
        }  
        return c;  
    }
    
    public static void main(String[] args) {
        AreaFenceHandler handler = new AreaFenceHandler();

        long st = System.currentTimeMillis();
        Random randomx = new Random(100);
        Random randomy = new Random(110);
        double xdot = randomx.nextDouble();
        double ydot = randomy.nextDouble();
        for (int i = 0; i < 500; i++) {
            double x = 118-randomx.nextInt(50)+xdot;
            double y = 42-randomx.nextInt(20)+ydot;
            List<String> strings = handler.areaIds(new Coordinate(x, y));
            if (null != strings) {

//                System.out.println(strings.size());
                if (strings.size() <=5) {
                    System.out.println(x+","+y);
                    for (String string : strings) {
                        System.out.print(string+"/");
                    }
                    System.out.println();
                    System.out.println();
                }
            } else {
//                System.out.println("x:"+x+",y:"+y);
            }
        }
        long ed = System.currentTimeMillis();

        System.out.println("time:"+(ed-st));
    }
}
