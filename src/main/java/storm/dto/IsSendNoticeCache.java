package storm.dto;
//用来缓存某些通知是否已经发送了通知，比如gps未定位报文满足了10帧，并且时间满足连续3小时，则发送通知
public class IsSendNoticeCache {
	public boolean gpsIsSend;
	public boolean canIsSend;
	public boolean socIsSend;
}
