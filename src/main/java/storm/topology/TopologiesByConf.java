package storm.topology;

import storm.bolt.deal.KafkaSendBolt;
import storm.bolt.deal.cusmade.CarNoticelBolt;
import storm.bolt.deal.cusmade.UserActionBolt;
import storm.bolt.deal.norm.AlarmBolt;
import storm.bolt.deal.norm.EleFenceBolt;
import storm.bolt.deal.norm.FaultBolt;
import storm.bolt.deal.norm.FilterBolt;
import storm.bolt.deal.norm.SynEsculBolt;

import java.util.Properties;
import storm.util.ConfigUtils;
import storm.kafka.scheme.ErrorDataScheme;
import storm.kafka.scheme.RealinfoScheme;
import storm.kafka.scheme.RegScheme;
import storm.kafka.KafkaConfig;
import storm.system.SysDefine;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologiesByConf {
	public static void main(String[] args) throws Exception{
        Config conf = new Config();
        readSysDefine(conf);

        KafkaConfig kafkaRealinfoConfig = new KafkaConfig(SysDefine.VEH_REALINFO_DATA_TOPIC, SysDefine.ZKROOT, SysDefine.VEH_REALINFO_GROUPID, SysDefine.ZK_HOSTS, new RealinfoScheme());
        kafkaRealinfoConfig.setZKConfig(SysDefine.ZKPORT, SysDefine.ZKSERVERS.split(","));

        KafkaConfig kafkaErrordataConfig = new KafkaConfig(SysDefine.ERROR_DATA_TOPIC, SysDefine.ZKROOT, SysDefine.ERROR_DATA_GROUPID, SysDefine.ZK_HOSTS, new ErrorDataScheme());
        kafkaRealinfoConfig.setZKConfig(SysDefine.ZKPORT, SysDefine.ZKSERVERS.split(","));

        KafkaConfig kafkaRegConfig = new KafkaConfig(SysDefine.PLAT_REG_TOPIC, SysDefine.ZKROOT, SysDefine.PLAT_REG_GROUPID, SysDefine.ZK_HOSTS, new RegScheme());
        kafkaRegConfig.setZKConfig(SysDefine.ZKPORT, SysDefine.ZKSERVERS.split(","));
        kafkaRegConfig.setOutputStreamId(SysDefine.REG_STREAM_ID);

        int realSpoutNo=Integer.valueOf(ConfigUtils.sysDefine.getProperty("storm.kafka.spout.no"));
        int workerNo=Integer.valueOf(ConfigUtils.sysDefine.getProperty("storm.worker.no"));
        int boltNo=Integer.valueOf(ConfigUtils.sysDefine.getProperty("storm.worker.bolt.no"));
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SysDefine.REALINFO_SPOUT_ID, new KafkaSpout(kafkaRealinfoConfig.getSpoutConfig()), realSpoutNo);//KAFKA实时报文消息
        builder.setSpout(SysDefine.ERRORDATA_SPOUT_ID, new KafkaSpout(kafkaErrordataConfig.getSpoutConfig()), realSpoutNo);//KAFKA错误报文消息
        builder.setSpout(SysDefine.REG_SPOUT_ID, new KafkaSpout(kafkaRegConfig.getSpoutConfig()), realSpoutNo);//KAFKA平台注册报文消息

        builder.setBolt(SysDefine.CHECKFILTER_BOLT_ID, new FilterBolt(), boltNo).setNumTasks(boltNo*3)
                .fieldsGrouping(SysDefine.ERRORDATA_SPOUT_ID, new Fields(SysDefine.VID))//接收车辆错误报文数据
                .fieldsGrouping(SysDefine.REALINFO_SPOUT_ID, new Fields(SysDefine.VID));//接收车辆实时数据

        builder.setBolt(SysDefine.ALARM_BOLT_ID, new AlarmBolt(), boltNo*3).setNumTasks(boltNo*9)//预警处理
                .fieldsGrouping(SysDefine.CHECKFILTER_BOLT_ID, SysDefine.SPLIT_GROUP, new Fields(SysDefine.VID));//预警的车辆实时数据

        builder.setBolt(SysDefine.FAULT_BOLT_ID, new FaultBolt(), boltNo*3).setNumTasks(boltNo*9)//故障处理
        .fieldsGrouping(SysDefine.ALARM_BOLT_ID, SysDefine.FAULT_GROUP, new Fields(SysDefine.VID));//故障实时数据

        builder.setBolt(SysDefine.FENCE_BOLT_ID, new EleFenceBolt(), boltNo*3).setNumTasks(boltNo*9)//电子围栏告警处理
        .fieldsGrouping(SysDefine.CHECKFILTER_BOLT_ID, SysDefine.FENCE_GROUP, new Fields(SysDefine.VID));//电子围栏告警实时数据
       
        builder.setBolt(SysDefine.CUS_NOTICE_BOLT_ID, new CarNoticelBolt(), boltNo*3).setNumTasks(boltNo*9)//soc 与超时处理
        .fieldsGrouping(SysDefine.CHECKFILTER_BOLT_ID, SysDefine.CUS_NOTICE_GROUP, new Fields(SysDefine.VID));//soc 与超时处理实时数据

        builder.setBolt(SysDefine.YAACTION_BOLT_ID, new UserActionBolt(), boltNo*3).setNumTasks(boltNo*9)//雅安用户行为处理
        .fieldsGrouping(SysDefine.CHECKFILTER_BOLT_ID, SysDefine.YAACTION_GROUP, new Fields(SysDefine.VID));//雅安用户行为实时数据
        
        builder.setBolt(SysDefine.SYNES_BOLT_ID, new SynEsculBolt(), boltNo*3).setNumTasks(boltNo*9)//es数据同步处理
        .fieldsGrouping(SysDefine.ALARM_BOLT_ID, SysDefine.SYNES_GROUP, new Fields(SysDefine.VID))//预警信息通知同步数据
        .fieldsGrouping(SysDefine.CHECKFILTER_BOLT_ID, SysDefine.SYNES_GROUP, new Fields(SysDefine.VID))//电子围栏告警实时数据
        .noneGrouping(SysDefine.REG_SPOUT_ID,SysDefine.REG_STREAM_ID);

//        builder.setBolt(SysDefine.QUICK_BOLT_ID, new QuickCacheBolt(), boltNo*2).setNumTasks(boltNo*6)//历史补发相关计算
//        .fieldsGrouping(SysDefine.CHECKFILTER_BOLT_ID, SysDefine.SUPPLY_GROUP, new Fields(SysDefine.VID));

        builder.setBolt(SysDefine.KAFKASEND_BOLT_ID, new KafkaSendBolt(), boltNo*2).setNumTasks(boltNo*6)//发送kafka消息，必要时可以动态增加线程数以增加发送线程数据
                .fieldsGrouping(SysDefine.ALARM_BOLT_ID, SysDefine.VEH_ALARM, new Fields(SysDefine.VID))//车辆告警数据
                .fieldsGrouping(SysDefine.ALARM_BOLT_ID, SysDefine.VEH_ALARM_REALINFO_STORE, new Fields(SysDefine.VID))//车辆报警状态、实时需要存储的数据
        		.fieldsGrouping(SysDefine.FENCE_BOLT_ID, SysDefine.FENCE_ALARM, new Fields(SysDefine.VID))//电子围栏
        		.fieldsGrouping(SysDefine.FAULT_BOLT_ID, SysDefine.FAULT_STREAM, new Fields(SysDefine.VID))//故障处理
        		.fieldsGrouping(SysDefine.YAACTION_BOLT_ID, SysDefine.YAACTION_NOTICE, new Fields(SysDefine.VID))//雅安公交驾驶行为
        		.fieldsGrouping(SysDefine.SYNES_BOLT_ID, SysDefine.SYNES_NOTICE, new Fields(SysDefine.VID))//es 同步推送
        		.fieldsGrouping(SysDefine.CUS_NOTICE_BOLT_ID, SysDefine.CUS_NOTICE, new Fields(SysDefine.VID))//告警 同步推送
                .fieldsGrouping(SysDefine.CHECKFILTER_BOLT_ID, SysDefine.HISTORY, new Fields(SysDefine.VID));//历史数据直接存储
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);
        conf.setMaxSpoutPending(1000);
        conf.setNumWorkers(workerNo);
        conf.setNumAckers(0);//设置不需要应答
        StormSubmitter.submitTopology("qyallStorm", conf, builder.createTopology());
//        new Thread(new Runnable() {
//			@Override
//			public void run() {
//				RedisTotalCacheInitUtil.init();
//			}
//		});
    }

    private static void readSysDefine(Config conf){
        try {
        	Properties p = ConfigUtils.sysDefine;
            SysDefine.VEH_REALINFO_DATA_TOPIC = p.getProperty("kafka.topic.veh_realinfo_data");
            SysDefine.ZKROOT = p.getProperty("kafka.zkroot");
            SysDefine.VEH_REALINFO_GROUPID = p.getProperty("kafka.metadata.veh_realinfo_groupid");
            SysDefine.ZK_HOSTS = p.getProperty("kafka.zk.hosts");
            SysDefine.BROKER_HOSTS = p.getProperty("kafka.broker.hosts");
            SysDefine.ZKPORT = Integer.valueOf(p.getProperty("kafka.zkPort"));
            SysDefine.ZKSERVERS = p.getProperty("kafka.zkServers");
            SysDefine.VEH_TEST_GROUPID = p.getProperty("kafka.metadata.veh_test_groupid");
            SysDefine.ERROR_DATA_TOPIC = p.getProperty("kafka.topic.errordatatopic");
            SysDefine.ERROR_DATA_GROUPID = p.getProperty("kafka.metadata.veh_error_groupid");
            SysDefine.PLAT_REG_TOPIC = p.getProperty("kafka.platform.veh.reg");
            SysDefine.PLAT_REG_GROUPID = p.getProperty("kafka.platform.group");

            conf.put("kafka.customer.hosts", p.getProperty("kafka.broker.hosts"));
            conf.put("print.log", p.getProperty("print.log"));

            conf.put("redis.host", p.getProperty("redis.host"));
            conf.put("redis.port", p.getProperty("redis.port"));
            conf.put("redis.pass", p.getProperty("redis.pass"));
            conf.put("redis.maxActive", p.getProperty("redis.maxActive"));
            conf.put("redis.maxIdle", p.getProperty("redis.maxIdle"));
            conf.put("redis.maxWait", p.getProperty("redis.maxWait"));
            conf.put("redis.timeOut", p.getProperty("redis.timeOut"));
            conf.put("redis.timeInterval", p.getProperty("redis.timeInterval"));
            conf.put("redis.listenInterval", p.getProperty("redis.listenInterval"));
            conf.put("ctfo.cacheHost", p.getProperty("ctfo.cacheHost"));
            conf.put("ctfo.cachePort", p.getProperty("ctfo.cachePort"));
            conf.put("ctfo.cacheDB", p.getProperty("ctfo.cacheDB"));
            conf.put("ctfo.cacheTable", p.getProperty("ctfo.cacheTable"));
            conf.put("kafka.topic.alarm", p.getProperty("kafka.topic.alarm"));
            conf.put("kafka.topic.alarmstore", p.getProperty("kafka.topic.alarmstore"));
            conf.put("kafka.topic.realinfostore", p.getProperty("kafka.topic.realinfostore"));
            conf.put("kafka.writer.no", p.get("kafka.writer.no"));
            conf.put("storm.kafka.spout.no", p.get("storm.kafka.spout.no"));
            conf.put("storm.worker.bolt.no", p.get("storm.worker.bolt.no"));
            conf.put("storm.worker.no", p.get("storm.worker.no"));
            conf.put("alarm.frame.cache", p.get("alarm.frame.cache"));
            conf.put("producer.poolNo", p.get("producer.poolNo"));
            conf.put("producer.againNo", p.get("producer.againNo"));
            conf.put("redis.offline.time", p.get("redis.offline.time"));
            conf.put("redis.offline.checktime", p.get("redis.offline.checktime"));
            conf.put("kafka.topic.action", p.get("kafka.topic.action"));
            conf.put("kafka.topic.fencealarm", p.get("kafka.topic.fencealarm"));
            conf.put("db.cache.flushtime", p.get("db.cache.flushtime"));
            conf.put("kafka.topic.customfault", p.get("kafka.topic.customfault"));
            conf.put("kafka.topic.es.status", p.get("kafka.topic.es.status"));
            conf.put("offline.check.time", p.get("offline.check.time"));
            conf.put("kafka.platform.veh.reg", p.get("kafka.platform.veh.reg"));
            conf.put("redis.cluster.data.syn", p.get("redis.cluster.data.syn"));
            conf.put("kafka.topic.notice", p.get("kafka.topic.notice"));
            conf.put("inidle.timeOut.check.time", p.get("inidle.timeOut.check.time"));
            conf.put("alarm.continue.counts", p.get("alarm.continue.counts"));
            conf.put("es.send.time", p.get("es.send.time"));
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}

