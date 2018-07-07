package storm.handler.fence.input;

public class OpenDoorAlarmRule extends RulePoJo implements CustomAlarmRule {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    RuleType type;

    public String stopType;//区域内告警in，区域外内告警out，区域外内外均告警 inout
    {
        setType(RuleType.RESULT.setResultType(Boolean.class));
    }
    @Override
    public RuleType getType() {

        return type;
    }

    void setType(RuleType type) {
        this.type = type;
    }


}
