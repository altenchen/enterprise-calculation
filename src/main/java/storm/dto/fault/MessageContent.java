package storm.dto.fault;

import java.io.Serializable;
import java.util.UUID;

public class MessageContent implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1560001L;
	public String msgId;//msg uuid
	public long startTime;//开始时间
	public long endTime;//结束时间
	public long time;//消息报文时间
	public long sustainTime;//持续时间
	public int triggerCounter;//发生次数
	public int risk;//消息级别
	public int status;//1开始 2 持续中 3 结束

	public MessageContent(long startTime) {
		this(getUUID(),startTime);
	}

	public MessageContent(String id, long startTime) {
		super();
		this.msgId = id;
		this.startTime = startTime;
		this.risk = -999;
		initZero();
	}

	public void initZero(){
		this.endTime = 0L;//结束时间
		this.time = 0L;//消息报文时间
		this.sustainTime = 0L;//持续时间
		this.triggerCounter = 0;
	}
	public void addCounter(){
		triggerCounter=triggerCounter+1;
	}
	
	public static String getUUID(){
		return UUID.randomUUID().toString().replace("-", "");
	}
}
