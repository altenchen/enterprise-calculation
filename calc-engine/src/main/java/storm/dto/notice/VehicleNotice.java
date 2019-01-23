package storm.dto.notice;

/**
 * 车辆通知
 *
 * @author 智杰
 */
public class VehicleNotice {

    /**
     * 车辆ID
     */
    protected String vid;

    /**
     * 车辆VIN
     */
    protected String vin;

    /**
     * 通知ID
     */
    protected String msgId;

    /**
     * 通知类型
     */
    protected String msgType;

    /**
     * 车辆经纬度
     */
    protected String location;

    /**
     * 车辆SOC
     */
    protected String soc;

    /**
     * 车辆里程
     */
    protected String mileage;

    /**
     * 车辆通知状态
     */
    protected String status;

    /**
     * 平台处理时间
     */
    protected String noticeTime;

    //region get & set方法

    public String getVid() {
        return vid;
    }

    public void setVid(final String vid) {
        this.vid = vid;
    }

    public String getVin() {
        return vin;
    }

    public void setVin(final String vin) {
        this.vin = vin;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(final String msgId) {
        this.msgId = msgId;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(final String msgType) {
        this.msgType = msgType;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(final String location) {
        this.location = location;
    }

    public String getSoc() {
        return soc;
    }

    public void setSoc(final String soc) {
        this.soc = soc;
    }

    public String getMileage() {
        return mileage;
    }

    public void setMileage(final String mileage) {
        this.mileage = mileage;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(final String status) {
        this.status = status;
    }

    public String getNoticeTime() {
        return noticeTime;
    }

    public void setNoticeTime(final String noticeTime) {
        this.noticeTime = noticeTime;
    }

    //endregion
}
