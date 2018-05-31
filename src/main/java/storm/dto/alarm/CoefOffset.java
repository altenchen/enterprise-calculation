package storm.dto.alarm;

public class CoefOffset {

	public String item;//数据项
	public int type;//数据项类型，数值 、数组,0数值，1数组
	public double coef;//系数
	public double offset;//偏移值
	public int isCustom;//自定义数据项，1是自定义，0非自定义
	public CoefOffset(String item, int type, double coef, double offset) {
		super();
		this.item = item;
		this.type = type;
		this.offset = offset;
		this.coef = coef;
	}
	public CoefOffset(String item, int type, double coef, double offset, int isCustom) {
		super();
		this.item = item;
		this.type = type;
		this.coef = coef;
		this.offset = offset;
		this.isCustom = isCustom;
	}
	
}
                                                  