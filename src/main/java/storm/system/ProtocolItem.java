package storm.system;

/**
 * 协议项
 */
public final class ProtocolItem {

    public static final String ICCID = "ICCID";//新的报文定义 给web 字段为 ICCID
    public static final String REG_TYPE="TYPE"; //平台注册通知类型 0:从未上过线，1:车机终端上线 ，2:车机离线，3:平台上线，4:平台下线
	public static final String REG_STATUS="STATUS";//0,1
	public static final String PLAT_ID="PLATID";
	public static final String SEQ_ID ="SEQID";
	public static final String USERNAME ="USERNAME";
	public static final String PASSWORD ="PASSWORD";



    public static final String LOCATION="LOCATION";//GPS 经度，纬度
	
	public static final String CAN_LEN="4410021";//can 会话长度
	public static final String CAN_CONT="4410022";//can 会话内容
	public static final String MILE_DISTANCE="DISTANCE";//前后2帧里程值之差
	public static final String GPS_ADMIN_REGION="GPS_REGION";//行政区域

}
