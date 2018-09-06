package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 *
 */
public interface OnOffInfoNotice extends InfoNotice {

    /**
     全局扫描，扫描所有车辆
     * @param type
     * @param status
     * @param now
     * @param timeout
     * @return
     */
    List<Map<String, Object>> fulldoseNotice(String type, ScanRange status, long now, long timeout);//status:0全量数据，status:1活跃数据，status:2其他定义

    /**
     * 生成通知
     * @param dat
     * @param now
     * @param timeout
     * @return
     */
    Map<String, Object> generateNotices(ImmutableMap<String, String> dat, long now, long timeout);

    /**
     * 此方法检查离线
     * @param type
     * @param status
     * @param now
     * @param timeout
     */
    void onOffCheck(String type, int status, long now, long timeout);
}
