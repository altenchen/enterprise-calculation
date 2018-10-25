package storm.topology;

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-10-23
 * @description:
 */
public final class TopologiesForDebug {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(TopologiesForDebug.class);

    /**
     * http://storm.apache.org/releases/current/index.html
     * @param args 拓扑启动参数, 忽略.
     * @throws Exception 拓扑启动异常
     */
    public static void main(String[] args) throws Exception {
//        args = new String[]{"E://sysDefine.properties"};
        LocalCluster cluster = new LocalCluster();
        LOG.info("本地集群启动");
        TopologiesByConf.submitTopology(args, cluster::submitTopology);
        TimeUnit.DAYS.sleep(1);
        cluster.shutdown();
        LOG.info("本地集群关闭");
    }
}
