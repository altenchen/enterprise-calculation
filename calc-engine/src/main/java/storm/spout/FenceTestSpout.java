package storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 电子围栏测试数据
 *
 * @author 智杰
 */
public class FenceTestSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;

    private String vin = "LMPG2PAB8JCS00011";
    private String vid = "852a6923-ad9d-475d-b76d-9be46d901131";

    /**
     * 港湾大道路线
     */
    private List<String[]> coordinates = new ArrayList<String[]>() {{
        add(initCoordinates(new BigDecimal("113.563423"), new BigDecimal("22.373218")));
        add(initCoordinates(new BigDecimal("113.569345"), new BigDecimal("22.372662")));
        add(initCoordinates(new BigDecimal("113.57604"), new BigDecimal("22.373456")));
        add(initCoordinates(new BigDecimal("113.580846"), new BigDecimal("22.372821")));
        add(initCoordinates(new BigDecimal("113.586511"), new BigDecimal("22.370598")));
        add(initCoordinates(new BigDecimal("113.592434"), new BigDecimal("22.368138")));
        add(initCoordinates(new BigDecimal("113.597154"), new BigDecimal("22.366312")));
        add(initCoordinates(new BigDecimal("113.601446"), new BigDecimal("22.364645")));
        add(initCoordinates(new BigDecimal("113.601103"), new BigDecimal("22.361708")));
        add(initCoordinates(new BigDecimal("113.599558"), new BigDecimal("22.358137")));
        add(initCoordinates(new BigDecimal("113.597583"), new BigDecimal("22.355438")));
        add(initCoordinates(new BigDecimal("113.596124"), new BigDecimal("22.352183")));
        add(initCoordinates(new BigDecimal("113.594064"), new BigDecimal("22.34877")));
    }};

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");


    @Override
    public void open(final Map map, final TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        coordinates.forEach(coords -> {
            String data = "SUBMIT 0 " + vin + " REALTIME {VID:" + vid + ",2502:" + coords[0] + ",2503:" + coords[1] + ",VTYPE:402881f266f1a8960166f2471e160c59,2000:20181211174750,CTYPE:1_1_1,2101:60,2102:1,2103:MTo1OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OF81OA==,2201:0,2202:2230,2203:0,2204:0,2205:0,2208:0,2209:0,2213:1,2214:2,2301:3,2307:1,2308:MjMwOToxLDIzMTA6MywyMzAyOjU4LDIzMDM6MjAwMDAsMjMxMToyMDAwMCwyMzA0OjYwLDIzMDU6Mzk3NCwyMzA2OjEwMDAw,2501:0,2601:1,2602:1,2603:3312,2604:1,2605:1,2606:3312,2607:1,2608:1,2609:58,2610:1,2611:1,2612:58,2613:3974,2614:10000,2615:91,2617:1999,2804:0,2805:,2808:0,2809:,2920:0,2921:0,2922:,2923:0,2924:,3201:2,3801:4095,7101:60,7102:1,7103:Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6Ojo6,7615:91,2001:120,2002:1,2003:MTozMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMl8zMzEyXzMzMTJfMzMxMg==,7001:120,7002:1,7003:DPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8AzwDPAM8Azw,9999:" + dateFormat.format(new Date()) + "}";
            spoutOutputCollector.emit(new Values(vid, data));
            Utils.sleep(10000);
        });
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("vid", "data"));
    }

    private static BigDecimal num = new BigDecimal("1000000");

    private static String[] initCoordinates(BigDecimal x, BigDecimal y) {
        BigDecimal xDec = x;
        BigDecimal xRes = xDec.multiply(num).setScale(0, RoundingMode.HALF_DOWN);

        BigDecimal yDec = y;
        BigDecimal yRes = yDec.multiply(num).setScale(0, RoundingMode.HALF_DOWN);
        return new String[]{xRes.intValue() + "", yRes.intValue() + ""};
    }

    public static double distance(double n1, double e1, double n2, double e2) {
        double jl_jd = 102834.74258026089786013677476285;
        double jl_wd = 111712.69150641055729984301412873;
        double b = Math.abs((e1 - e2) * jl_jd);
        double a = Math.abs((n1 - n2) * jl_wd);
        return Math.sqrt((a * a + b * b));
    }

    public static void main(String[] args) {
        System.out.println(distance(113.584848,22.371114, 113.596446,22.365538));
    }

}
