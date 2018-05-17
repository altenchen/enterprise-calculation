package storm.dto;

import java.io.Serializable;

public class FillChargeCar implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 29000006L;
	
	public String vid;//车辆唯一识别码
	public double longitude;//经度
	public double latitude;//纬度
	public String lastOnline;//yyyyMMddhhmmss
	public int running=0;//yyyyMMddhhmmss
	public FillChargeCar(String vid, double longitude, double latitude, String lastOnline) {
		super();
		this.vid = vid;
		this.longitude = longitude;
		this.latitude = latitude;
		this.lastOnline = lastOnline;
	}
	public FillChargeCar(String vid, double longitude, double latitude, String lastOnline, int running) {
		this(vid,longitude,latitude,lastOnline);
		this.running = running;
	}
	
}
