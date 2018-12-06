package storm.domain.fence.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.Fence;
import storm.domain.fence.area.AreaCron;
import storm.domain.fence.area.Circle;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.area.Polygon;
import storm.domain.fence.cron.Cron;
import storm.domain.fence.cron.DailyCycle;
import storm.domain.fence.cron.DailyOnce;
import storm.domain.fence.cron.WeeklyCycle;
import storm.domain.fence.event.DriveInside;
import storm.domain.fence.event.DriveOutside;
import storm.domain.fence.event.EventCron;
import storm.domain.fence.service.IFenceQueryService;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.SqlUtils;

import java.sql.Date;
import java.sql.Time;
import java.util.*;

/**
 * 电子围栏查询接口实现-MYSQL
 *
 * @author 智杰
 */
public class FenceQueryMysqlServiceImpl implements IFenceQueryService {

    private static final SqlUtils SQL_UTILS = SqlUtils.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(FenceQueryMysqlServiceImpl.class);
    /**
     * 上一次同步围栏规则时间
     */
    private long prevSyncTime = 0L;

    /**
     * 车辆与围栏规则
     * <vid, <fenceId, 围栏规则>>
     */
    private Map<String, ImmutableMap<String, Fence>> vehicleFenceRuleMap = new HashMap<>(0);
    /**
     * 电子围栏与规则(事件)映射关系
     * <fenceId, [eventId, eventId, ...]>
     */
    private Map<String, Set<String>> fenceEventMap = new HashMap<>(0);
    /**
     * 电子围栏与车辆映射关系
     * <fenceId, [vid, vid, ...]>
     */
    private Map<String, Set<String>> fenceVehicleMap = new HashMap<>(0);

    /**
     * 刷新电子围栏缓存
     */
    private synchronized void refresh() {
        if (vehicleFenceRuleMap == null) {
            LOGGER.info("首次同步电子围栏规则");
            //第一次同步规则
            queryFenceRules();
            LOGGER.info("首次同步电子围栏规则结束");
            return;
        }
        long flushTime = ConfigUtils.getSysDefine().getDbCacheFlushTime() * 1000;
        long currentTime = System.currentTimeMillis();
        if (currentTime - this.prevSyncTime >= flushTime) {
            LOGGER.info("同步电子围栏规则");
            //到了刷新时间，重新同步规则
            queryFenceRules();
            LOGGER.info("同步电子围栏规则结束");
        }
    }

    /**
     * 查询围栏规则规则
     */
    private void queryFenceRules() {
        String sql = ConfigUtils.getSysParam().getFenceSql();
        Map<String, ImmutableMap<String, Fence>> fences = SQL_UTILS.query(sql, resultSet -> {
            //车辆与围栏映射关系 <vid, <fenceId, 围栏规则>>
            Map<String, ImmutableMap<String, Fence>> vehicleFenceRuleMap = new HashMap<>(10);
            //围栏与事件映射关系 <fenceId, [eventId, eventId, ...]>
            Map<String, Set<String>> fenceEventMap = new HashMap<>(10);
            //围栏与车辆映射关系 <fenceId, [vid, vid, ...]>
            Map<String, Set<String>> fenceVehicleMap = new HashMap<>(0);
            String key = null;
//            ImmutableMap<String, Fence> fenceList = new ArrayList<>(10);
            ImmutableMap.Builder<String, Fence> fenceList = new ImmutableMap.Builder<>();
            while (resultSet.next()) {
                //车辆VID
                String vid = resultSet.getString("VID");
                //围栏ID
                String fenceId = resultSet.getString("FENCE_ID");
                //规则类型1、驶离；2、驶入
                int ruleType = resultSet.getInt("RULE_TYPE");
                //周期类型1、单次执行；2、每周循环；3、每天循环
                int periodType = resultSet.getInt("PERIOD_TYPE");
                //开始日期
                Date startDate = resultSet.getDate("START_DATE");
                //结束日期
                Date endDate = resultSet.getDate("END_DATE");
                //星期，多个之间用的逗号分隔，周一为1到周日为7
                String week = resultSet.getString("WEEK");
                //开始启用时间【时分秒】
                Time startTime = resultSet.getTime("START_TIME");
                //结束启用时间【时分秒】
                Time endTime = resultSet.getTime("END_TIME");
                //1、圆形；2、多边形
                int chartType = resultSet.getInt("CHART_TYPE");
                //经纬度范围【1圆形时=半径;圆点， 2多边形时=每一个;的值为经纬度点】
                String lonlatRange = resultSet.getString("LONLAT_RANGE");

                //因为根据VID来排序的， 如果VID变了就把结果保存起来
                if (key == null) {
                    key = vid;
                } else if (!key.equals(vid)) {
                    ImmutableMap<String, Fence> res = fenceList.build();
                    LOGGER.info("VID:{} 围栏规则有 SIZE:{}", key, res.size());
                    String mapKey = StringUtils.isEmpty(key) ? "ALL" : key;
                    vehicleFenceRuleMap.put(mapKey, res);
                    fenceList = new ImmutableMap.Builder<>();
                    key = vid;
                }

                //初始化电子围栏区域
                ImmutableMap<String, AreaCron> areas = initFenceArea(chartType, lonlatRange);

                //初始化规则列表
                ImmutableMap<String, EventCron> events = initFenceEvent(ruleType);

                //初始化执行计划
                ImmutableList<Cron> cron = initFenceCron(periodType, week, startDate, endDate, startTime, endTime);

                Fence fence = new Fence(fenceId, areas, events, cron);
                fenceList.put(fenceId, fence);

                //添加围栏与事件映射关系
                fenceEventMap.getOrDefault(fenceId, new HashSet<>()).addAll(events.keySet());
                //添加围栏与车辆映射关系
                fenceVehicleMap.getOrDefault(fenceId, new HashSet<>()).add(vid);
            }

            ImmutableMap<String, Fence> res = fenceList.build();
            if (!res.isEmpty()) {
                LOGGER.info("VID:{} 围栏规则有 SIZE:{}", key, res.size());
                String mapKey = StringUtils.isEmpty(key) ? "ALL" : key;
                vehicleFenceRuleMap.put(mapKey, res);
            }

            this.fenceEventMap = fenceEventMap;
            this.fenceVehicleMap = fenceVehicleMap;
            return vehicleFenceRuleMap;
        });
        if (MapUtils.isEmpty(fences)) {
            LOGGER.info("查询不到围栏规则 SQL:{}", sql);
        }
        this.vehicleFenceRuleMap = fences;
        //更新最后一次同步时间
        this.prevSyncTime = System.currentTimeMillis();
    }

    /**
     * 初始化电子围栏事件
     *
     * @param ruleType 1、驶离；2、驶入；3、驶入驶离
     * @return 电子围栏事件
     */
    private ImmutableMap<String, EventCron> initFenceEvent(int ruleType) {
        ImmutableMap.Builder<String, EventCron> events = new ImmutableMap.Builder<>();
        switch (ruleType) {
            case 1:
                //驶离
                events.put(SysDefine.FENCE_OUTSIDE_EVENT_ID, new DriveInside(SysDefine.FENCE_OUTSIDE_EVENT_ID, null));
                break;
            case 2:
                //驶入
                events.put(SysDefine.FENCE_INSIDE_EVENT_ID, new DriveOutside(SysDefine.FENCE_INSIDE_EVENT_ID, null));
                break;
            case 3:
                //驶入驶离
                events.put(SysDefine.FENCE_INSIDE_EVENT_ID, new DriveInside(SysDefine.FENCE_INSIDE_EVENT_ID, null));
                events.put(SysDefine.FENCE_OUTSIDE_EVENT_ID, new DriveOutside(SysDefine.FENCE_OUTSIDE_EVENT_ID, null));
                break;
            default:
                break;
        }
        return events.build();
    }

    /**
     * 初始化围栏执行计划
     * weeFlag：
     * 周【1,3,5,7】 weeFlag二进制标志位 00101011(空六五四三二一日)
     * 周【1,2,3】 weeFlag二进制标志位 00001110(空六五四三二一日)
     * 周【4,5,6】 weeFlag二进制标志位 01110000(空六五四三二一日)
     *
     * @param periodType 周期类型1、单次执行；2、每周循环；3、每天循环
     * @param week       星期，多个之间用的逗号分隔，周一为1到周日为7
     * @param startDate  例：2017-02-21
     * @param endDate    例：2017-02-21
     * @param startTime  例：23:22:22
     * @param endTime    例：23:22:22
     * @return 返回执行计划列表
     */
    private ImmutableList<Cron> initFenceCron(int periodType, String week, Date startDate, Date endDate, Time startTime, Time endTime) {
        ImmutableList.Builder<Cron> cronBuilder = new ImmutableList.Builder<>();
        switch (periodType) {
            case 1:
                //单次执行
                cronBuilder.add(new DailyOnce(startDate.getTime(), endDate.getTime(), startTime.getTime(), endTime.getTime()));
                break;
            case 2:
                //每周循环
                String[] weekStringArray = week.split(",");
                if (weekStringArray.length == 0) {
                    break;
                }
                int weekFlag = -1;
                for (String weekString : weekStringArray) {
                    if (StringUtils.isEmpty(weekString)) {
                        continue;
                    }
                    int weekNumber = Integer.valueOf(weekString);
                    if (weekFlag == -1) {
                        weekFlag = 1 << (weekNumber % 7);
                    } else {
                        weekFlag = weekFlag | 1 << (weekNumber % 7);
                    }
                }
                cronBuilder.add(new WeeklyCycle((byte) weekFlag, startTime.getTime(), endTime.getTime()));
                break;
            case 3:
                //每天执行
                cronBuilder.add(new DailyCycle(startTime.getTime(), endTime.getTime()));
                break;
            default:
                break;
        }
        return cronBuilder.build();
    }

    /**
     * 初始化围栏区域
     *
     * @param chartType   1、圆形；2、多边形
     * @param lonlatRange 经纬度【1圆形(半径;坐标) 8352;116.59574,39.916867】【2多边形(坐标;坐标;...) 116.524329,39.828861;116.60398,39.828861;116.60398,39.776637;116.524329,39.776637】
     * @return 返回围栏区域
     */
    private ImmutableMap<String, AreaCron> initFenceArea(int chartType, String lonlatRange) {
        ImmutableMap.Builder<String, AreaCron> areas = new ImmutableMap.Builder<>();
        String areaId = SysDefine.FENCE_AREA_ID;
        //坐标值按逗号拆分后,length为2
        int coordinateSize = 2;
        switch (chartType) {
            case 1:
                //圆形区域
                String[] splitArray = lonlatRange.split(";");
                if (splitArray.length < coordinateSize) {
                    break;
                }
                String[] coordinateArray = splitArray[1].split(",");
                if (coordinateArray.length < coordinateSize) {
                    break;
                }
                areas.put(areaId, new Circle(areaId, new Coordinate(Double.valueOf(coordinateArray[0]), Double.valueOf(coordinateArray[1])), Double.valueOf(splitArray[0]), null));
                break;
            case 2:
                //多边形区域
                ImmutableList.Builder<Coordinate> coordinates = new ImmutableList.Builder();
                Arrays.stream(lonlatRange.split(";")).forEach(item -> {
                    String[] coordinateArrays = item.split(",");
                    if (coordinateArrays.length < coordinateSize) {
                        return;
                    }
                    coordinates.add(new Coordinate(Double.valueOf(coordinateArrays[0]), Double.valueOf(coordinateArrays[1])));
                });
                areas.put(areaId, new Polygon(areaId, coordinates.build(), null));
                break;
            default:
                break;
        }
        return areas.build();
    }

    /**
     * 查询围栏列表
     *
     * @param vid 车辆ID
     * @return 车辆对应的围栏列表
     */
    @Override
    public ImmutableMap<String, Fence> query(String vid) {
        refresh();
        return vehicleFenceRuleMap.get(vid);
    }

    @Override
    public boolean existFence(String fenceId) {
        return fenceEventMap.containsKey(fenceId);
    }

    @Override
    public boolean existFenceEvent(String fenceId, String eventId) {
        return fenceEventMap.containsKey(fenceId) && fenceEventMap.get(fenceId).contains(eventId);
    }

    @Override
    public boolean existFenceVehicle(String fenceId, String vehicleId) {
        return fenceVehicleMap.containsKey(fenceId) && fenceVehicleMap.get(fenceId).contains(vehicleId);
    }
}
