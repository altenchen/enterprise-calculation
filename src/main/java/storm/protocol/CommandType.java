package storm.protocol;

/**
 * 终端接入交互协议
 * @author: xzp
 * @date: 2018-06-05
 * @description: 定义私有协议的命令常量
 *
 * 终端消息格式说明
 * 1.	参数间使用一个空格为分隔符。
 * 2.	消息结构：消息前缀 序列号 VIN码 命令标识 参数集。
 * 3.	前缀主要有SUBMIT和REPORT两种，SUBMIT表示主动发送，REPORT表示应答。
 * 4.	序列号主要用于下行指令的状态通知匹配，主动上传的消息序列号默认为0。
 * 5.	参数集包含多个参数，单个参数以KEY:VALUE形式，多个参数以半角逗号分隔。
 */
public final class CommandType {

    /**
     * TBOX 主动发送命令
     */
    public static final String SUBMIT = "SUBMIT";
    /**
     * TBOX 被动应答命令
     */
    public static final String REPORT = "REPORT";

    /**
     * 1.原始报文
     */
    public static final String SUBMIT_PACKET = "PACKET";

    /**
     * 2.链接状态通知
     */
    public static final String SUBMIT_LINKSTATUS = "LINKSTATUS";

    /**
     * 3.终端注册消息
     */
    public static final String SUBMIT_LOGIN = "LOGIN";

    /**
     * 4.实时信息上报
     */
    public static final String SUBMIT_REALTIME = "REALTIME";

    /**
     * 5.状态信息上报
     */
    public static final String SUBMIT_TERMSTATUS = "TERMSTATUS";

    /**
     * 6.补发信息上报
     */
    public static final String SUBMIT_HISTORY = "HISTORY";

    /**
     * 7.车辆运行状态
     */
    public static final String SUBMIT_CARSTATUS = "CARSTATUS";

    /**
     * 8.查询命令
     */
    public static final String SUBMIT_GETARG = "GETARG";

    /**
     * 9.查询应答
     */
    public static final String REPORT_GETARG = "GETARG";

    /**
     * 10.设置命令
     */
    public static final String SUBMIT_SETARG = "SETARG";

    /**
     * 11.设置应答
     */
    public static final String REPORT_SETARG = "SETARG";

    /**
     * 12.终端控制
     */
    public static final String SUBMIT_CONTROL = "CONTROL";

    /**
     * 13.控制应答
     */
    public static final String REPORT_CONTROL = "CONTROL";

    /**
     * 14.车辆转发通知
     */
    public static final String SUBMIT_FORWARD = "FORWARD";

    /**
     * 15.命令标识不可识别数据
     */
    public static final String SUBMIT_UNKNOW = "UNKNOW";

    /**
     * 16.终端锁车报文
     */
    public  static final String SUBMIT_TERMLOCK = "TERMLOCK";
}
