package storm.handler.fence.input;

import java.util.Arrays;

public class SpeedAlarmRule extends RulePoJo implements CustomAlarmRule {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    RuleType type;

    public String speedType;//大于 gt，小于lt，介于两者之间bt,大于大值或者小于小值 glt
    public double[]speeds;//[10,90]
    {
        speeds=new double[2];
        setType(RuleType.RESULT.setResultType(Boolean.class));
    }
    @Override
    public RuleType getType() {

        return type;
    }

    void setType(RuleType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "SpeedAlarmRule [type=" + type + ", speedType=" + speedType + ", speeds=" + Arrays.toString(speeds)
                + "]";
    }
    public static void main(String[] args) {
        SpeedAlarmRule rule = new SpeedAlarmRule();
        rule.setType(RuleType.VOID.setResultType(Boolean.class));
        System.out.println(rule.getType().getResultType());

        System.out.println(rule instanceof CustomAlarmRule);
    }

}
