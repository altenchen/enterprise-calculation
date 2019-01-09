package storm.dto.notice;

/**
 * 无CAN通知
 *
 * @author 智杰
 */
public class NoCanNotice extends VehicleAlarmNotice {

    /**
     * 触发开始通知时的持续时长
     */
    protected String sdelay;

    /**
     * 触发结束通知时的持续时长
     */
    protected String edelay;

    /**
     * TODO 目前先兼容下游消息格式，后面统一格式再移除
     */
    protected String noticetime;

    //region get & set方法

    public String getSdelay() {
        return sdelay;
    }

    public void setSdelay(final String sdelay) {
        this.sdelay = sdelay;
    }

    public String getEdelay() {
        return edelay;
    }

    public void setEdelay(final String edelay) {
        this.edelay = edelay;
    }

    public String getNoticetime() {
        return noticetime;
    }

    public void setNoticetime(final String noticetime) {
        this.noticetime = noticetime;
    }

    //endregion

}
