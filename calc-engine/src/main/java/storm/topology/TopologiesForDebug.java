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

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * 运行前先配置下
 * Run/Debug Configurations --> TopologiesForDebug --> Program arguments --> 粘贴sysDefine_debug.properties路径
 * @author: xzp
 * @date: 2018-10-23
 * @description:
 */
public final class TopologiesForDebug {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(TopologiesForDebug.class);

    /**
     * http://storm.apache.org/releases/current/index.html
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
            new DebugEmitSpout()
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
