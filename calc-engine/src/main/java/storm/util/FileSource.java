package storm.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import storm.dto.area.AreaFence;
import storm.dto.area.FenceIndex;
import storm.dto.fence.Coordinate;

public class FileSource {

    public static List<AreaFence> areaFences;
    public static Map<String,AreaFence> areaIdFenceCache = null;

    static {
        try {
            initArea();
            initCache();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<String> readFile(){

        InputStream in = null;
        List<String> lines = new LinkedList<String>();
        try {

            in = FileSource.class.getClassLoader().getResourceAsStream("area.gps");
            BufferedReader reader =  new BufferedReader(new InputStreamReader(in, "UTF-8"));
            String line = null;

            while (null != (line = reader.readLine())) {
                if (!"".equals(line)) {

                    lines.add(line);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if (lines.size()>0) {
            return lines;
        }
        return null;
    }

    private static void initArea(){
        List<String> lines = readFile();
        if (null != lines) {
            Set<String> areaIds =  new HashSet<String>();
            List<AreaFence> fences = new ArrayList<AreaFence>(lines.size());

            for (String line : lines) {
                String [] areagps = line.trim().split("#");
                if (2 == areagps.length) {
                    String area = areagps[0].trim();
                    String gpses = areagps[1].trim();
                    if (null != area && null != gpses) {

                        String [] areas = area.split("_");
                        String [] locations = gpses.split(";");

                        int len = locations.length;
                        //gps大于3才能有区域，而不是一个线。
                        if (2 == areas.length && len >= 3) {
                            String areaId = new String(areas[0]);
                            String areaName = new String(areas[1]);
                            List<Coordinate> coordinates =  new ArrayList<Coordinate>(len);

                            for (int idx = 0; idx < len;idx++) {
                                String [] location = locations[idx].split(",");
                                if (2 == location.length) {

                                    double x = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(location[0]) ? location[0] : "0");
                                    double y = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(location[1]) ? location[1] : "0");

                                    Coordinate coord = new Coordinate(x, y);
                                    coordinates.add(coord);
                                }
                            }
                            //index为最大最小经纬度，相当于一个矩形
                            FenceIndex index = new FenceIndex(coordinates);

                            AreaFence fence = new AreaFence(areaId, areaName, index);
                            fences.add(fence);
                            areaIds.add(areaId);
                        }
                    }
                }
            }

            if (areaIds.size() > 0) {
                sortAndParent(areaIds, fences);
                areaFences = fences;
            }

        }
    }

    static void initCache(){
        int len = areaFences.size();
        if (len >0) {
            areaIdFenceCache = new HashMap<String,AreaFence>();
        }
        for (int i = 0; i < len; i++) {
            AreaFence fence = areaFences.get(i);
            areaIdFenceCache.put(fence.areaId, fence);
        }
    }


    private static void sortAndParent(Set<String> areaIds,List<AreaFence> fences){

        if (null != areaIds && null != fences) {

            int len = fences.size();
            //冒泡排序法，按照xmin进行从大到小排序
            for (int idx = 0; idx < len-1; idx++) {
                AreaFence fence = fences.get(idx);
                //每次从idx+1到len中找到最小的一个
                for (int byidx = idx+1; byidx < len; byidx++) {
                    AreaFence byfence = fences.get(byidx);

                    if (fence.index.xmin > byfence.index.xmin) {
                        fences.set(idx, byfence);
                        fences.set(byidx, fence);
                        fence = fences.get(idx);
                    }
                }

            }
            for (int idx = 0; idx < len; idx++) {
                AreaFence fence = fences.get(idx);
                setParentArea(fence, areaIds);
            }
        }

    }

    private static void setParentArea(AreaFence fence,Set<String> areaIds){
        if (null != fence && null != fence.areaId
                && fence.areaId.length() ==6
                && null == fence.parentAreaId
                && null != areaIds) {

            String areaId = fence.areaId;
            String cyds = areaId.substring(2,6);
            //如果不是省级才去设置parentArea
            if (! "0000".equals(cyds)) {

                String prov = areaId.substring(0,2);
                String dist = areaId.substring(4,6);

                String parentAreaId = null;
                //如果是市级区域（如：123400）
                if ("00".equals(dist)) {
                    parentAreaId = prov+"0000";
                } else {
                    String provcity = areaId.substring(0,4);
                    parentAreaId = provcity+"00";
                }

                if (null != parentAreaId
                        && areaIds.contains(parentAreaId)) {
                    fence.setParentAreaId(parentAreaId);
                } else {
                    parentAreaId = prov+"0000";
                    if (areaIds.contains(parentAreaId)) {
                        fence.setParentAreaId(parentAreaId);
                    }
                }
            }

        }
    }
    public static void main(String[] args) {
        int len = areaFences.size();
        System.out.println(len);
        for (int i = 0; i < len; i++) {
            System.out.println(areaFences.get(i).index.xmin);
        }
        System.out.println();
    }
}
