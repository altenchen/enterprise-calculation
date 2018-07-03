package storm.util;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import ent.calc.util.ConfigUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.StringUtils;

/**
 * kafka发送工具集
 */
public class KafkaSendUtils {
	private static final ConfigUtils configUtils = ConfigUtils.getInstance();
	private static BlockingQueue<Producer<byte[], byte[]>> queue;
	private static ThreadLocal<Producer<byte[], byte[]>> localThread;
	private static ProducerConfig config;
	private static int poolNo=32;
	private static int againNo=8;
	static{
		init(poolNo);//队列中加入32个 Producer，方便调用
	}
	private static void init(int pool){
		init();
		queue = new LinkedBlockingQueue<Producer<byte[], byte[]>>(pool);
        for (int i = 0; i < pool; i++) {
        	queue.offer(new Producer<byte[], byte[]>(config));
		}

	}
	private static void init(){
		Properties properties = new Properties();
        properties.put("metadata.broker.list", configUtils.sysDefine.getProperty("kafka.broker.hosts"));
        properties.put("request.required.acks", "0");
        properties.put("producer.type", "async");
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
        properties.put("partitioner.class", "storm.kafka.MyPartitioner");
        
        config = new ProducerConfig(properties);
        localThread=new ThreadLocal<Producer<byte[], byte[]>>();
        
        String pool= configUtils.sysDefine.getProperty("producer.poolNo");
        String again= configUtils.sysDefine.getProperty("producer.againNo");
		if(!StringUtils.isEmpty(pool)) {
			poolNo=Integer.valueOf(pool);
		}
		if(!StringUtils.isEmpty(again)) {
			againNo=Integer.valueOf(again);
		}
        	
	}
	/**
	 * 此方法未经测试不知道会不会长久不调用，关闭连接的情况。
	 * @return
	 */
	public static Producer<byte[], byte[]> getPoolProducer(){
		if(null!=localThread.get()) {
			return localThread.get();
		}
		if (queue.size()<=againNo) {
			for (int i = 0; i < poolNo-againNo; i++) {
	        	queue.offer(new Producer<byte[], byte[]>(config));
			}
		}
		return queue.poll();
	}

	/**
	 * 此方法如果海量调用会浪费系统资源，必须在代码中关闭连接或者返回连接
	 * @return
	 */
	public static Producer<byte[], byte[]> getNewProducer(){
		Producer<byte[], byte[]> producer=new Producer<byte[], byte[]>(config);
		try {
			TimeUnit.SECONDS.sleep(1);
			localThread.set(producer);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return producer;
	}
	
	/**
	 * 如果当前线程有 返回当前线程的连接
	 * @return
	 */
	public static Producer<byte[], byte[]> getDefaultProducer(){
		if(null==localThread.get()) {
			localThread.set(new Producer<byte[], byte[]>(config));
		}
		return localThread.get();
	}
	
	public static void returnProducerConn(Producer<byte[], byte[]> producer){
		
		localThread.set(producer);
		
//		localThread.remove();
//		queue.offer(producer);
		
	}
}
