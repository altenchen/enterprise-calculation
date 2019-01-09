package storm.dto.notice;

/**
 * SOC过低通知
 *
 * @author 智杰
 */
public class LowSocNotice extends VehicleAlarmNotice {

    /**
     * 兼容性处理
     * 原通知中的遗留
     */
    protected String lowSocThreshold;

    //region get & set方法

    public String getLowSocThreshold() {
        return lowSocThreshold;
    }

    public void setLowSocThreshold(final String lowSocThreshold) {
        this.lowSocThreshold = lowSocThreshold;
    }

    //endregion
}
