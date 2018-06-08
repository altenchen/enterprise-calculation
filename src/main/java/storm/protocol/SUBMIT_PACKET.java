package storm.protocol;

import storm.system.DataKey;

/**
 * @author: xzp
 * @date: 2018-06-05
 * @description: 原始报文
 */
public final class SUBMIT_PACKET {

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
        return "0".equals(verifyState);
    }

    /**
     * 是否校验失败
     * @param verifyState 校验状态
     * @return 是否校验失败
     */
    public static final boolean isVerifyFailure(String verifyState) {
        return "1".equals(verifyState);
    }
}
