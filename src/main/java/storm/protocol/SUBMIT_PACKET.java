package storm.protocol;

import storm.system.DataKey;

/**
 * @author: xzp
 * @date: 2018-06-05
 * @description: 原始报文
 */
public final class SUBMIT_PACKET {
    /**
     * 车辆ID, 车辆在平台中唯一编码
     */
    public static final String VEHICLE_ID = DataKey.VEHICLE_ID;

    /**
     * 车辆类型, 预警用于匹配约束条件
     */
    public static final String VEHICLE_TYPE = "VTYPE";

    /**
     * 车机类型, 接入标识_协议种类_协议版本, 可通过CommandUtils工具类解析.
     * 接入标识:1.前置机接入 2.平台转发接入
     * 协议种类:1.国标协议 2.地标协议
     * 协议版本:1.0.0
     */
    public static final String CAR_TYPE = "CTYPE";

    /**
     * 消息类型, 见车载终端协议《命令标识定义》表
     */
    public static final String MESSAGE_TYPE = "1";

    /**
     * 原始报文, 报文字节数组经过base64编码
     */
    public static final String BASE64_DATA = "2";

    /**
     * 上传时间, 格式'YYYYmmddHHMMSS'
     */
    public static final String UPLOAD_TIME = "3";

    /**
     * 校验状态, 0-成功, 1-失败.
     */
    public static final String VERIFY_STATE = "4";

    /**
     * 是否校验成功
     * @param verifyState 校验状态
     * @return 是否校验成功
     */
    public static final boolean isVerifySucess(String verifyState) {
        return "0" == verifyState;
    }

    /**
     * 是否校验失败
     * @param verifyState 校验状态
     * @return 是否校验失败
     */
    public static final boolean isVerifyFailure(String verifyState) {
        return "1" == verifyState;
    }
}
