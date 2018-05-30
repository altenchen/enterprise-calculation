package storm.util;

public class GpsUtil {
	private static final double EARTH_RADIUS = 6378.137;
	//弧度
	private static final double RADIAN = Math.PI / 180.0;
	
	/**
	 * 以地球赤道半径求得弧长即为gps距离，弧长公式nπr/180
	 * @param lng1 经度
	 * @param lat1 纬度
	 * @param lng2 经度
	 * @param lat2 纬度
	 * @return 两点 之间的gps 距离
	 */
	public static final double getDistance(double lng1, double lat1, double lng2, double lat2){
	    double radLat1 = lat1* RADIAN;
	    double radLat2 = lat2* RADIAN;
	    double a = radLat1 - radLat2;
	    double b = lng1* RADIAN - lng2* RADIAN;
	    double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) + 
	    Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)));
	    s = s * EARTH_RADIUS;
	    s = Math.round(s * 10000) / 10000;
	    return s;
	}
}
