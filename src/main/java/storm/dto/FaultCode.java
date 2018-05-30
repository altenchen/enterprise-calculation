package storm.dto;

public class FaultCode {

	public String id;
	public String code;
	public int level;
	public int type;
	public FaultCode(String id,String code, int level, int type) {
		super();
		this.id = id;
		this.code = code;
		this.level = level;
		this.type = type;
	}
}
                                                  