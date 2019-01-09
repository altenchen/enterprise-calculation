package storm.dto.notice;

/**
 * 点火熄火通知
 *
 * @author 智杰
 */
public class IgniteShutNotice extends VehicleAlarmNotice {

    /**
     * 点火熄火期间最大车速
     */
    protected String maxSpeed;

    /**
     * 点火熄火期间 SOC 差值
     */
    protected String energy;

    /**
     * TODO 目前先兼容下游消息格式，后面统一格式再移除
     */
    protected String noticetime;

    //region get & set方法

    public String getMaxSpeed() {
        return maxSpeed;
    }

    public void setMaxSpeed(final String maxSpeed) {
        this.maxSpeed = maxSpeed;
    }

    public String getEnergy() {
        return energy;
    }

    public void setEnergy(final String energy) {
        this.energy = energy;
    }

    public String getNoticetime() {
        return noticetime;
    }

    public void setNoticetime(final String noticetime) {
        this.noticetime = noticetime;
    }


    //endregion

}
