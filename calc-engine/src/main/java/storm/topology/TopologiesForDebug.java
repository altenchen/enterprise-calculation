package storm.topology;

import org.apache.storm.LocalCluster;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.debug.DebugEmitSpout;
import storm.debug.DebugSendBolt;
import storm.util.function.TeConsumer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * 运行前先配置下
 * Run/Debug Configurations --> TopologiesForDebug --> Program arguments --> 粘贴sysDefine_debug.properties路径
 *
 * @author: xzp
 * @date: 2018-10-23
 * @description:
 */
public final class TopologiesForDebug {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(TopologiesForDebug.class);


    private static BigDecimal num = new BigDecimal("1000000");

    /**
     * 港湾大道路线
     */
    private static List<String[]> coordinates = new ArrayList<String[]>() {{
        add(initCoordinates(new BigDecimal("113.556832"), new BigDecimal("22.374849")));
        add(initCoordinates(new BigDecimal("113.56018"), new BigDecimal("22.373499")));
        add(initCoordinates(new BigDecimal("113.56387"), new BigDecimal("22.372547")));
        add(initCoordinates(new BigDecimal("113.568248"), new BigDecimal("22.372785")));
        add(initCoordinates(new BigDecimal("113.572883"), new BigDecimal("22.373261")));
        add(initCoordinates(new BigDecimal("113.576573"), new BigDecimal("22.373817")));
        add(initCoordinates(new BigDecimal("113.579492"), new BigDecimal("22.373341")));
        add(initCoordinates(new BigDecimal("113.583097"), new BigDecimal("22.372071")));
        add(initCoordinates(new BigDecimal("113.586358"), new BigDecimal("22.370801")));
        add(initCoordinates(new BigDecimal("113.588933"), new BigDecimal("22.369372")));
        add(initCoordinates(new BigDecimal("113.592195"), new BigDecimal("22.36842")));
        add(initCoordinates(new BigDecimal("113.59537"), new BigDecimal("22.36707")));
        add(initCoordinates(new BigDecimal("113.601893"), new BigDecimal("22.364689")));
        add(initCoordinates(new BigDecimal("113.601121"), new BigDecimal("22.361673")));
        add(initCoordinates(new BigDecimal("113.599233"), new BigDecimal("22.358657")));
        add(initCoordinates(new BigDecimal("113.598031"), new BigDecimal("22.354926")));
        add(initCoordinates(new BigDecimal("113.595885"), new BigDecimal("22.352227")));
        add(initCoordinates(new BigDecimal("113.593825"), new BigDecimal("22.349607")));
        add(initCoordinates(new BigDecimal("113.59228"), new BigDecimal("22.345717")));
        add(initCoordinates(new BigDecimal("113.592967"), new BigDecimal("22.341113")));
    }};

    private static String[] initCoordinates(BigDecimal x, BigDecimal y) {
        BigDecimal xDec = x;
        BigDecimal xRes = xDec.multiply(num).setScale(0, RoundingMode.HALF_DOWN);

        BigDecimal yDec = y;
        BigDecimal yRes = yDec.multiply(num).setScale(0, RoundingMode.HALF_DOWN);
        return new String[]{xRes.intValue() + "", yRes.intValue() + ""};
    }

    /**
     * http://storm.apache.org/releases/current/index.html
     *
     * @param args 拓扑启动参数, 忽略.  编辑run config --> program arguments --> 添加debug.properties路径
     * @throws Exception 拓扑启动异常
     */
    public static void main(String[] args) throws Exception {
        LocalCluster cluster = new LocalCluster();
        LOG.info("本地集群启动");
        TopologiesByConf.submitTopology(
            args,
            cluster::submitTopology,
            TopologiesForDebug::buildDebugEmitSpout,
            TopologiesForDebug::buildDebugSendBolt
        );
        TimeUnit.DAYS.sleep(1);
        cluster.shutdown();
        LOG.info("本地集群关闭");
    }

    private static void buildDebugEmitSpout(
        @NotNull final TeConsumer<@NotNull String, @NotNull String, @NotNull IRichSpout> buildMessageEmitSpout) {

        buildMessageEmitSpout.accept(
            DebugEmitSpout.getComponentId(),
            DebugEmitSpout.getGeneralStreamId(),
            new DebugEmitSpout("4240f821-89aa-46af-9f7b-22c0d93351ca", "CS123456789012345", coordinates)
        );
    }

    private static void buildDebugSendBolt(
        @NotNull final BiConsumer<@NotNull String, @NotNull IRichBolt> buildMessageSendBolt) {

        buildMessageSendBolt.accept(
            DebugSendBolt.getComponentId(),
            new DebugSendBolt()
        );
    }
}
