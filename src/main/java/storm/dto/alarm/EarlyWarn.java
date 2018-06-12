package storm.dto.alarm;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * @author wza
 * 预警规则获取
 */
public class EarlyWarn implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1999980001L;
	public String id;//规则Id
	public String name;//规则名称
	public String vehModelId;//车辆类型Id
	public int levels;//告警级别
	public String dependId;//依赖Id
	public String left1DataItem;//左一数据项
	public String leftExpression;//左表达式
	public String left2DataItem;//左二数据项
	public String middleExpression;//中间表达式
	public double right1Value;//右一值
	public double right2Value;//右二值
	
	public boolean isAllCommon;//是否适用所有车型
	
	/***   暂时不生效，等前端变业务 开始   ***/
	public List<EarlyWarn> earlyWarns;//依赖规则
	public int judgeCount = 10;//连续多少次开始结束报警
	public long judgeTime = -1;//连续发生多少时间开始结束报警
	/***   暂时不生效，等前端变业务 结束   ***/
	
	public EarlyWarn(String id, String name, String vehModelId, int levels, String dependId, String left1DataItem,
			String leftExpression, String left2DataItem, String middleExpression, double right1Value,
			double right2Value) {
		super();
		this.id = id;
		this.name = name;
		this.vehModelId = vehModelId;
		this.levels = levels;
		this.dependId = dependId;
		this.left1DataItem = left1DataItem;
		this.leftExpression = leftExpression;
		this.left2DataItem = left2DataItem;
		this.middleExpression = middleExpression;
		this.right1Value = right1Value;
		this.right2Value = right2Value;
		
		setCommon(vehModelId);
	}
	
	public EarlyWarn(String id, String vehModelId, String left1DataItem, String leftExpression, String left2DataItem,
			String middleExpression, double right1Value, double right2Value) {
		super();
		this.id = id;
		this.vehModelId = vehModelId;
		this.left1DataItem = left1DataItem;
		this.leftExpression = leftExpression;
		this.left2DataItem = left2DataItem;
		this.middleExpression = middleExpression;
		this.right1Value = right1Value;
		this.right2Value = right2Value;
		
		setCommon(vehModelId);
	}
	
	void setCommon(String vehModelId){
		if (null == vehModelId 
				|| "".equals(vehModelId.trim())
				|| "ALL".equals(vehModelId.trim())) {
			
			this.isAllCommon = true;
			
		} else {
			
			this.isAllCommon = false;
		}
	}
	
	/***   暂时不生效，等前端变业务 开始   ***/
	/**
	 * 此方法暂时不用，等前端业务变了以后再使用
	 * @param common
	 * @param commonCount
	 * @param count
	 */
	void setJudgeCondition(boolean common,int commonCount,int count){
		if (common) {
			this.judgeCount = commonCount;
		} else {
			this.judgeCount = count;
		}
	}
	
	void setDependWarns(List<EarlyWarn> earlyWarns){
		this.earlyWarns = earlyWarns;
	}
	
	void addDependWarns(EarlyWarn warn){
		if (null == warn) 
			return;
		
		if (null == earlyWarns) 
			earlyWarns = new LinkedList<EarlyWarn>();
		
		if (!earlyWarns.contains(warn)) {
			earlyWarns.add(warn);
		}
	}
	/***   暂时不生效，等前端变业务 结束   ***/
	
}
