package storm.dto.alarm;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import storm.system.DataKey;
import storm.util.dbconn.Conn;

/**
 * @author wza
 * 偏移处理
 */
public class CoefOffsetGetter {

    private static ConcurrentHashMap<String, CoefOffset> itemCoefOffsets;
    private static HashSet<String> gbItems;
    private static HashSet<String> cusItems;
    private static Conn conn ;
    private static List<Object[]>allValidCoefOffsets;
    static {
        itemCoefOffsets = new ConcurrentHashMap<String, CoefOffset>();
        gbItems = new HashSet<String>();
        cusItems = new HashSet<String>();
        initGb();
        try {
            conn = new Conn();
            allValidCoefOffsets = conn.getAllCoefOffset();
            initCus(allValidCoefOffsets);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void initGb(){

        CoefOffset offset = new CoefOffset("2302", 0, 1, 40);
        itemCoefOffsets.put("2302", offset);
        gbItems.add("2302");

        offset = new CoefOffset("2304", 0, 1, 40);
        itemCoefOffsets.put("2304", offset);
        gbItems.add("2304");

        offset = new CoefOffset("2402", 0, 1, 40);
        itemCoefOffsets.put("2402", offset);
        gbItems.add("2402");

        offset = new CoefOffset("2404", 0, 1, 40);
        itemCoefOffsets.put("2404", offset);
        gbItems.add("2404");

        offset = new CoefOffset("2406", 0, 1, 40);
        itemCoefOffsets.put("2406", offset);
        gbItems.add("2406");

        offset = new CoefOffset("2407", 0, 1, 40);
        itemCoefOffsets.put("2407", offset);
        gbItems.add("2407");

        offset = new CoefOffset("2609", 0, 1, 40);
        itemCoefOffsets.put("2609", offset);
        gbItems.add("2609");

        offset = new CoefOffset("2612", 0, 1, 40);
        itemCoefOffsets.put("2612", offset);
        gbItems.add("2612");

        offset = new CoefOffset("2103", 0, 1, 40);
        itemCoefOffsets.put("2103", offset);
        gbItems.add("2103");

        offset = new CoefOffset("2306", 0, 10, 10000);
        itemCoefOffsets.put("2306", offset);
        gbItems.add("2306");

        offset = new CoefOffset(DataKey._2614_TOTAL_ELECTRICITY, 0, 10, 10000);
        itemCoefOffsets.put(DataKey._2614_TOTAL_ELECTRICITY, offset);
        gbItems.add(DataKey._2614_TOTAL_ELECTRICITY);

        offset = new CoefOffset("2114", 0, 1, 40);
        itemCoefOffsets.put("2114", offset);
        gbItems.add("2114");

        offset = new CoefOffset("2115", 0, 1, 40);
        itemCoefOffsets.put("2115", offset);
        gbItems.add("2115");

        offset = new CoefOffset("2110", 0, 10, 0);
        itemCoefOffsets.put("2110", offset);
        gbItems.add("2110");

        offset = new CoefOffset("2111", 0, 10, 0);
        itemCoefOffsets.put("2111", offset);
        gbItems.add("2111");

        offset = new CoefOffset("2119", 0, 10, 0);
        itemCoefOffsets.put("2119", offset);
        gbItems.add("2119");

        offset = new CoefOffset("2003", 1, 1000, 0);
        itemCoefOffsets.put("2003", offset);
        gbItems.add("2003");

        offset = new CoefOffset("2201", 0, 10, 0);
        itemCoefOffsets.put("2201", offset);
        gbItems.add("2201");

        offset = new CoefOffset("2202", 0, 10, 0);
        itemCoefOffsets.put("2202", offset);
        gbItems.add("2202");

        offset = new CoefOffset("2305", 0, 10, 0);
        itemCoefOffsets.put("2305", offset);
        gbItems.add("2305");

        offset = new CoefOffset("2403", 0, 5, 0);
        itemCoefOffsets.put("2403", offset);
        gbItems.add("2403");

        offset = new CoefOffset("2405", 0, 100, 0);
        itemCoefOffsets.put("2405", offset);
        gbItems.add("2405");

        offset = new CoefOffset("2408", 0, 10, 0);
        itemCoefOffsets.put("2408", offset);
        gbItems.add("2408");

        offset = new CoefOffset("2410", 0, 500, 0);
        itemCoefOffsets.put("2410", offset);
        gbItems.add("2410");

        offset = new CoefOffset("2412", 0, 2, 0);
        itemCoefOffsets.put("2412", offset);
        gbItems.add("2412");

        offset = new CoefOffset("2413", 0, 100, 0);
        itemCoefOffsets.put("2413", offset);
        gbItems.add("2413");

        offset = new CoefOffset("2502", 0, 1000000, 0);
        itemCoefOffsets.put("2502", offset);
        gbItems.add("2502");

        offset = new CoefOffset("2503", 0, 1000000, 0);
        itemCoefOffsets.put("2503", offset);
        gbItems.add("2503");

        offset = new CoefOffset("2504", 0, 10, 0);
        itemCoefOffsets.put("2504", offset);
        gbItems.add("2504");

        offset = new CoefOffset("2603", 0, 1000, 0);
        itemCoefOffsets.put("2603", offset);
        gbItems.add("2603");

        offset = new CoefOffset("2606", 0, 1000, 0);
        itemCoefOffsets.put("2606", offset);
        gbItems.add("2606");

        offset = new CoefOffset("2613", 0, 10, 0);
        itemCoefOffsets.put("2613", offset);
        gbItems.add("2613");

        offset = new CoefOffset("2616", 0, 10, 0);
        itemCoefOffsets.put("2616", offset);
        gbItems.add("2616");

    }

    static void initCus(List<Object[]> coefOffsets){

        try {
            HashSet<String> thisAddItems = null;
            if (null != coefOffsets && coefOffsets.size() >0) {
                thisAddItems = new HashSet<String>();
                for (Object[] objects : coefOffsets) {
                    if (null != objects && objects.length >=5) {
                        //SEQ_NO,IS_ARRAY,FACTOR,OFFSET,IS_CUSTOM
                        String item = (String)objects[0];//数据库获取的
                        if (!gbItems.contains(item)) {

                            Object arrObj = (null == objects[1]) ? 0 : objects[1];
                            int type = (int)arrObj;

                            Object factObj = (null == objects[2]) ? 1 : objects[2];
                            double coef = (double)factObj;

                            Object offsetObj = (null == objects[3]) ? 0 : objects[3];
                            double offset = (double)offsetObj;

                            Object cusObj = (null == objects[4]) ? 0 : objects[4];
                            int isCustom = (int)cusObj;

                            CoefOffset coefOffset = new CoefOffset(item, type, coef, offset, isCustom);
                            itemCoefOffsets.put(item, coefOffset);
                            cusItems.add(item);
                            thisAddItems.add(item);
                        }
                    }
                }
            }

            if (null != thisAddItems && thisAddItems.size() >0) {

                HashSet <String> needRemove = null;
                if (cusItems.size() > 0) {
                    needRemove = new HashSet<String>();
                    for (String item : cusItems) {
                        if (!thisAddItems.contains(item)) {
                            needRemove.add(item);
                        }
                    }
                }

                if (null != needRemove && needRemove.size() > 0) {
                    for (String item : needRemove) {
                        itemCoefOffsets.remove(item);
                        cusItems.remove(item);
                    }
                }

            } else {
                if (cusItems.size() > 0) {
                    for (String item : cusItems) {
                        itemCoefOffsets.remove(item);
                    }
                    cusItems.clear();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void rebuild(){

        try {
            if (null == conn) {
                conn = new Conn();
            }

            allValidCoefOffsets = conn.getAllCoefOffset();
            initCus(allValidCoefOffsets);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ConcurrentHashMap<String, CoefOffset> getItemCoefOffsets() {
        return itemCoefOffsets;
    }

    public static CoefOffset getCoefOffset(String item) {

        if (null == item || "".equals(item.trim())) {
            return null;
        }
        return itemCoefOffsets.get(item);
    }
    public static void main(String[] args) {
        System.out.println(itemCoefOffsets.size());
    }
}
