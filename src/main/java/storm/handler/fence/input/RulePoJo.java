package storm.handler.fence.input;

abstract public class RulePoJo implements Rule {

    /**
     *
     */
    private static final long serialVersionUID = 12366660L;

    String code;
    @Override
    public RuleType getType() {

        return RuleType.VOID.setResultType(null);
    }
    @Override
    public Object clone() throws CloneNotSupportedException{
        return this;
    }
    @Override
    public String getName() {

        return null;
    }
    @Override
    public String getRuleCondition() {

        return null;
    }
    @Override
    public String getScriptEngine() {

        return null;
    }
    @Override
    public String getCode() {

        return code;
    }
    public void setCode(String code) {

        this.code=code;
    }
}
