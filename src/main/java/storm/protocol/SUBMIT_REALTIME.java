package storm.protocol;

/**
 * @author: xzp
 * @date: 2018-06-05
 * @description: 实时信息上报
 * TODO: 变量命名
 */
public final class SUBMIT_REALTIME {

    /**
     * 车辆ID, 车辆在平台中唯一编码
     */
    public static final String VehicleId = "VID";

    /**
     * 车辆类型, 预警用于匹配约束条件
     */
    public static final String VehicleType = "VTYPE";

    /**
     * 车机类型, 接入标识_协议种类_协议版本, 可通过CommandUtils工具类解析.
     * 接入标识:1.前置机接入 2.平台转发接入
     * 协议种类:1.国标协议 2.地标协议
     * 协议版本:1.0.0
     */
    public static final String CarType = "CTYPE";

    /**
     * 采集时间
     */
    public static final String CollectTime = "2000";

    /**
     * 单体蓄电池总数
     */
    public static final String _2001 = "2001";

    /**
     * 动力蓄电池包总数
     */
    public static final String _2002 = "2002";

    /**
     * 单体蓄电池电压值列表
     */
    public static final String _2003 = "2003";

    /**
     * 蓄电池包温度探针总数
     */
    public static final String _2101 = "2101";

    /**
     * 蓄电池包总数
     */
    public static final String _2102 = "2102";

    /**
     * 温度值列表
     */
    public static final String _2103 = "2103";

    /**
     * 车速
     */
    public static final String _2201 = "2201";

    /**
     * 里程
     */
    public static final String _2202 = "2202";

    /**
     * 档位
     */
    public static final String _2203 = "2203";

    /**
     * 加速踏板行程值
     */
    public static final String _2208 = "2208";

    /**
     * 制动踏板行程值
     */
    public static final String _2209 = "2209";

    /**
     * 空调设定温度
     */
    public static final String _2210 = "2210";

    /**
     * 续驶里程
     */
    public static final String _2290 = "2290";

    /**
     * 充放电状态
     */
    public static final String _2301 = "2301";

    /**
     * 电机控制器温度
     */
    public static final String _2302 = "2302";

    /**
     * 电机转速
     */
    public static final String _2303 = "2303";

    /**
     * 电机温度
     */
    public static final String _2304 = "2304";

    /**
     * 电机电压
     */
    public static final String _2305 = "2305";

    /**
     * 电机母线电流
     */
    public static final String _2306 = "2306";

    /**
     * 发动机状态
     */
    public static final String _2401 = "2401";

    /**
     * ECU温度
     */
    public static final String _2402 = "2402";

    /**
     * 车辆电池电压
     */
    public static final String _2403 = "2403";

    /**
     * 发动机温度
     */
    public static final String _2404 = "2404";

    /**
     * 进气歧管气压
     */
    public static final String _2405 = "2405";

    /**
     * 进气温度
     */
    public static final String _2406 = "2406";

    /**
     * 废气排出温度
     */
    public static final String _2407 = "2407";

    /**
     * 燃料喷射压力
     */
    public static final String _2408 = "2408";

    /**
     * 燃料喷射量
     */
    public static final String _2409 = "2409";

    /**
     * 点火提前角
     */
    public static final String _2410 = "2410";

    /**
     * 曲轴转速
     */
    public static final String _2411 = "2411";

    /**
     * 油门开度
     */
    public static final String _2412 = "2412";

    /**
     * 定位状态
     */
    public static final String _2501 = "2501";

    /**
     * 经度
     */
    public static final String _2502 = "2502";

    /**
     * 维度
     */
    public static final String _2503 = "2503";

    /**
     * 速度
     */
    public static final String _2504 = "2504";

    /**
     * 方向
     */
    public static final String _2505 = "2505";

    /**
     * 最高电压动力蓄电池单体所在电池包序号
     */
    public static final String _2601 = "2601";

    /**
     * 最高电压单体蓄电池序号
     */
    public static final String _2602 = "2602";

    /**
     * 电池单体电压最高值
     */
    public static final String _2603 = "2603";

    /**
     * 最低电压动力蓄电池包序号
     */
    public static final String _2604 = "2604";

    /**
     * 最低电压单体蓄电池序号
     */
    public static final String _2605 = "2605";

    /**
     * 电池单体电压最低值
     */
    public static final String _2606 = "2606";

    /**
     * 最高温度动力蓄电池包序号
     */
    public static final String _2607 = "2607";

    /**
     * 最高温度探针序号
     */
    public static final String _2608 = "2608";

    /**
     * 最高温度值
     */
    public static final String _2609 = "2609";

    /**
     * 最低温度动力蓄电池包序号
     */
    public static final String _2610 = "2610";

    /**
     * 最低温度探针序号
     */
    public static final String _2611 = "2611";

    /**
     * 最低温度值
     */
    public static final String _2612 = "2612";

    /**
     * 总电压
     */
    public static final String _2613 = "2613";

    /**
     * 总电流
     */
    public static final String _2614 = "2614";

    /**
     * SOC1
     */
    public static final String _2615 = "2615";

    /**
     * SOC2
     */
    public static final String _7615 = "7615";

    /**
     * 剩余能量
     */
    public static final String _2616 = "2616";

    /**
     * 绝缘电阻
     */
    public static final String _2617 = "2617";

    /**
     * 自定义类型编码列表
     */
    public static final String _2701 = "2701";

    /**
     * 自定义类型数据列表
     */
    public static final String _2702 = "2702";

    /**
     * 动力蓄电池报警标志
     */
    public static final String _2801 = "2801";

    /**
     * 动力蓄电池其他故障总数
     */
    public static final String _2802 = "2802";

    /**
     * 动力蓄电池其他故障代码列表
     */
    public static final String _2803 = "2803";

    /**
     * 电机故障总数
     */
    public static final String _2804 = "2804";

    /**
     * 电机故障代码列表
     */
    public static final String _2805 = "2805";

    /**
     * 发动机故障总数
     */
    public static final String _2806 = "2806";

    /**
     * 发动机故障列表
     */
    public static final String _2807 = "2807";

    /**
     * 其他故障总数
     */
    public static final String _2808 = "2808";

    /**
     * 其他故障代码列表
     */
    public static final String _2809 = "2809";
}
