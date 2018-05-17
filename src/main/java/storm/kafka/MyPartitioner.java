package storm.kafka;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import org.apache.log4j.Logger;

/** 
 * 类名: MyPartitioner
 * 功能: TOD0
 * 
 * 时间: 2015-7-10 下午2:38:35
 * 
 * @author huangjincheng 
 * @version  
 * @since JDK 1.6 
 * 2015-7-10
 */  
public class MyPartitioner implements Partitioner{
	private static Logger logger = Logger.getLogger(MyPartitioner.class);
	private Random random = new Random(); 
	public MyPartitioner(VerifiableProperties props){
		super();
		logger.info("初始化自定义分区选择器");
	}
	@Override
	public int partition(Object key, int numPartitions) {
		//Utils.abs(key.hashCode) % numPartitions  默认实现
		int partition = 0;
		try {
			if (key == null) { 
				//随机选择分区
				partition = random.nextInt(numPartitions);
			} else {
				String vid=new String((byte[]) key,"UTF-8");
				//根据哈希code对分区数取模
				partition = Math.abs(vid.hashCode())%numPartitions;
				logger.debug(vid+":"+partition);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//返回分区索引，从0开始
		return partition;
	}
}
