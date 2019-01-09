package storm.dto.notice;

/**
 * 车辆报警通知
 *
 * @author 智杰
 */
public class VehicleAlarmNotice extends VehicleNotice {

    //region 报警开始属性

    /**
     * 触发开始通知的时间
     */
    protected String stime;

    /**
     * 触发开始通知时的SOC
     */
    protected String ssoc;

    /**
     * 触发开始通知时的里程
     */
    protected String smileage;

    /**
     * 触发开始通知的经纬度
     */
    protected String slocation;

    /**
     * 触发开始通知的指标
     */
    protected String sthreshold;

    /**
     * 触发开始通知时的持续帧数
     */
    protected String scontinue;

    /**
     * 触发开始通知时的持续时长
     */
    protected String slazy;

    //endregion

    //region 报警结束属性

    /**
     * 触发结束通知的时间
     */
    protected String etime;

    /**
     * 触发结束通知时的SOC
     */
    protected String esoc;

    /**
     * 触发结束通知时的里程
     */
    protected String emileage;

    /**
     * 触发结束通知的经纬度
     */
    protected String elocation;

    /**
     * 触发结束通知的指标
     */
    protected String ethreshold;

    /**
     * 触发结束通知时的持续帧数
     */
    protected String econtinue;

    /**
     * 触发结束通知时的持续时长
     */
    protected String elazy;

    //endregion

    //region get & set方法

    public String getStime() {
        return stime;
    }

    public void setStime(final String stime) {
        this.stime = stime;
    }

    public String getSsoc() {
        return ssoc;
    }

    public void setSsoc(final String ssoc) {
        this.ssoc = ssoc;
    }

    public String getSmileage() {
        return smileage;
    }

    public void setSmileage(final String smileage) {
        this.smileage = smileage;
    }

    public String getSlocation() {
        return slocation;
    }

    public void setSlocation(final String slocation) {
        this.slocation = slocation;
    }

    public String getSthreshold() {
        return sthreshold;
    }

    public void setSthreshold(final String sthreshold) {
        this.sthreshold = sthreshold;
    }

    public String getScontinue() {
        return scontinue;
    }

    public void setScontinue(final String scontinue) {
        this.scontinue = scontinue;
    }

    public String getSlazy() {
        return slazy;
    }

    public void setSlazy(final String slazy) {
        this.slazy = slazy;
    }

    public String getEtime() {
        return etime;
    }

    public void setEtime(final String etime) {
        this.etime = etime;
    }

    public String getEsoc() {
        return esoc;
    }

    public void setEsoc(final String esoc) {
        this.esoc = esoc;
    }

    public String getEmileage() {
        return emileage;
    }

    public void setEmileage(final String emileage) {
        this.emileage = emileage;
    }

    public String getElocation() {
        return elocation;
    }

    public void setElocation(final String elocation) {
        this.elocation = elocation;
    }

    public String getEthreshold() {
        return ethreshold;
    }

    public void setEthreshold(final String ethreshold) {
        this.ethreshold = ethreshold;
    }

    public String getEcontinue() {
        return econtinue;
    }

    public void setEcontinue(final String econtinue) {
        this.econtinue = econtinue;
    }

    public String getElazy() {
        return elazy;
    }

    public void setElazy(final String elazy) {
        this.elazy = elazy;
    }


    //endregion
}
