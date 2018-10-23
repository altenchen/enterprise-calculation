package storm.entity;

/**
 * redis 缓存的通知消息格式
 * @author xzj
 * @version 2018-10-19
 */
public class NoticeMessage {

    private String vid;
    private String msgId;
    private String msgType;
    private String stime;
    private String slocation;
    private String ruleId;
    private long faultCode;
    private String noticeTime;
    private int level;
    private int status;
    private String etime;
    private String elocation;
    private String faultId;
    private int analyzeType;

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getStime() {
        return stime;
    }

    public void setStime(String stime) {
        this.stime = stime;
    }

    public String getSlocation() {
        return slocation;
    }

    public void setSlocation(String slocation) {
        this.slocation = slocation;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public long getFaultCode() {
        return faultCode;
    }

    public void setFaultCode(long faultCode) {
        this.faultCode = faultCode;
    }

    public String getNoticeTime() {
        return noticeTime;
    }

    public void setNoticeTime(String noticeTime) {
        this.noticeTime = noticeTime;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getEtime() {
        return etime;
    }

    public void setEtime(String etime) {
        this.etime = etime;
    }

    public String getElocation() {
        return elocation;
    }

    public void setElocation(String elocation) {
        this.elocation = elocation;
    }

    public String getFaultId() {
        return faultId;
    }

    public void setFaultId(String faultId) {
        this.faultId = faultId;
    }

    public int getAnalyzeType() {
        return analyzeType;
    }

    public void setAnalyzeType(int analyzeType) {
        this.analyzeType = analyzeType;
    }

}
