package storm.dto.fault;

/**
 * @author wza
 * 故障规则处理
 */
public class RiskDef extends AbstractDef {

    public String id;
    public String faultRuleId;
    public RiskDef(String id,String faultRuleId,int level, int mintimes, int maxtimes, int minduration, int maxduration,boolean first) {
        super(level, mintimes, maxtimes, minduration, maxduration,first);
        this.id=id;
        this.faultRuleId=faultRuleId;
    }

    public RiskDef(String id,String faultRuleId,int level, int mintimes, int maxtimes, int minduration, int maxduration) {
        this(id,faultRuleId,level, mintimes, maxtimes, minduration, maxduration,false);
    }
    /**
     *
     */
    private static final long serialVersionUID = 1239000061L;
    @Override
    Object calcul(Object... objs) {
        if(null == objs || objs.length !=2)
            return null;
        try {
            int times=(Integer)objs[0];
            int duration=(Integer)objs[1];
            return super.cal(times, duration);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
