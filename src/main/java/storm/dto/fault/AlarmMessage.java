package storm.dto.fault;

/**
 * @author wza
 * 故障规则处理
 */
public class AlarmMessage extends MessageContent {

    /**
     *
     */
    private static final long serialVersionUID = 17000500000121L;

    String alarmRuleId;

    public AlarmMessage(String id,long startTime, String alarmRuleId) {
        super(id,startTime);
        this.alarmRuleId = alarmRuleId;
    }

    @Override
    public String toString() {
        return "AlarmMessage [alarmRuleId=" + alarmRuleId + ", msgId=" + msgId + ", startTime=" + startTime
                + ", endTime=" + endTime + ", time=" + time + ", sustainTime=" + sustainTime + ", triggerCounter="
                + triggerCounter + ", risk=" + risk + ", status=" + status + "]";
    }

}
