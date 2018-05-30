package storm.dto.alarm;

import java.io.Serializable;

public class TimeGroups implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1900000021L;
	String stime;
	String etime;
	public TimeGroups(String stime) {
		super();
		this.stime = stime;
	}
	void flush(){
		this.stime = null;
		this.etime = null;
	}
	public String getStime() {
		return stime;
	}
	public String getEtime() {
		return etime;
	}
	@Override
	public String toString() {
		return "TimeGroups [stime=" + stime + ", etime=" + etime + "]";
	}
}
