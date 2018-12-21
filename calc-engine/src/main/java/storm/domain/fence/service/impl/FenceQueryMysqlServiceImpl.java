package storm.domain.fence.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dao.DataToRedis;
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
import storm.extension.DateExtension;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.SqlUtils;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.util.*;

/**
 * 电子围栏查询接口实现-MYSQL
 *
 * @author 智杰
 */
public class FenceQueryMysqlServiceImpl extends AbstractFenceQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(FenceQueryMysqlServiceImpl.class);

    private static final SqlUtils SQL_UTILS = SqlUtils.getInstance();

    public FenceQueryMysqlServiceImpl() {
        super(new DataToRedis());
    }

    /**
     * 查询围栏规则规则
     */
    @Override
    protected void dataQuery(DataInitCallback dataInitCallback) {
        long start = System.currentTimeMillis();
        LOGGER.info("初始化车辆围栏缓存");
        //车辆与围栏映射关系 <vid, <fenceId, 围栏规则>>
        Map<String, ImmutableMap<String, Fence>> vehicleFenceRule = new HashMap<>(10);
        Map<String, ImmutableMap.Builder<String, Fence>> vehicleFenceRuleTemp = new HashMap<>(10);
        //围栏与事件映射关系 <fenceId, [eventId, eventId, ...]>
        Map<String, Set<String>> fenceEvent = new HashMap<>(10);
        //围栏与车辆映射关系 <fenceId, [vid, vid, ...]>
        Map<String, Set<String>> fenceVehicle = new HashMap<>(0);
        Map<String, Fence> fenceMap = new HashMap<>(10);

        //获取所有电子围栏数据
        List<Fence> fences = queryFences(fenceEvent);

        //查询电子围栏与车辆关联
        List<FenceVehicle> relations = queryFenceVehicleRelations();

        //生成围栏与车辆关系
        fences.forEach(fence -> relations.forEach(relation -> {
            if (!fence.getFenceId().equals(relation.getFenceId())) {
                return;
            }
            ImmutableMap.Builder<String, Fence> builder = vehicleFenceRuleTemp.getOrDefault(relation.getVid(), new ImmutableMap.Builder<>()).put(fence.getFenceId(), fence);
            vehicleFenceRuleTemp.put(relation.getVid(), builder);

            Set<String> vids = fenceVehicle.getOrDefault(fence.getFenceId(), new HashSet<>());
            vids.add(relation.getVid());
            fenceVehicle.put(fence.getFenceId(), vids);

            fenceMap.put(fence.getFenceId(), fence);

        }));

        //转成ImmutableMap
        vehicleFenceRuleTemp.forEach((key, value) -> {
            vehicleFenceRule.put(key, value.build());
        });

        //完成初始化
        dataInitCallback.finishInit(vehicleFenceRule, fenceEvent, fenceVehicle, fenceMap);
        LOGGER.info("初始化车辆围栏缓存 耗时: {} ms. 关联围栏的车辆数: {}. 围栏数: {}", System.currentTimeMillis() - start, vehicleFenceRule.size(), fences.size());
    }

    /**
     * 查询电子围栏与车辆关联
     *
     * @return
     */
    @NotNull
    private List<FenceVehicle> queryFenceVehicleRelations() {
        long start = System.currentTimeMillis();
        LOGGER.info("查询电子围栏与车辆关联");
        String sql = ConfigUtils.getSysParam().getFenceVehicleSql();
        List<FenceVehicle> result = SQL_UTILS.query(sql, resultSet -> {
            List<FenceVehicle> relations = new ArrayList<>();
            while (resultSet.next()) {
                relations.add(new FenceVehicle(resultSet.getString("VID"), resultSet.getString("FENCE_ID")));
            }
            return relations;
        });
        LOGGER.info("查询电子围栏与车辆关联 耗时: {} ms", System.currentTimeMillis() - start);
        if (result == null) {
            return new ArrayList<>();
        }
        return result;
    }

    /**
     * 查询电子围栏规则
     *
     * @param fenceEvent
     * @return
     */
    private List<Fence> queryFences(final Map<String, Set<String>> fenceEvent) {
        long start = System.currentTimeMillis();
        LOGGER.info("查询电子围栏规则");
        String sql = ConfigUtils.getSysParam().getFenceSql();
        List<Fence> result = SQL_UTILS.query(sql, resultSet -> {
            List<Fence> fences = new ArrayList<>();
            while (resultSet.next()) {
                //围栏ID
                String fenceId = null;
                try {
                    fenceId = resultSet.getString("FENCE_ID");
                    //规则类型1、驶离；2、驶入；3、驶入驶离
                    String ruleType = resultSet.getString("RULE_TYPE");
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
                    String chartType = resultSet.getString("CHART_TYPE");
                    //经纬度范围【1圆形时=半径;圆点， 2多边形时=每一个;的值为经纬度点】
                    String lonlatRange = resultSet.getString("LONLAT_RANGE");

                    //初始化电子围栏区域
                    ImmutableMap<String, AreaCron> areas = initFenceArea(chartType, lonlatRange);

                    //初始化规则列表
                    ImmutableMap<String, EventCron> events = initFenceEvent(fenceId, fenceEvent, ruleType);

                    //初始化执行计划
                    ImmutableList<Cron> cron = initFenceCron(fenceId, periodType, week, startDate, endDate, startTime, endTime);

                    Fence fence = new Fence(fenceId, areas, events, cron);
                    fences.add(fence);
                } catch (SQLException e) {
                    LOGGER.error("FENCE_ID:" + fenceId + " 电子围栏初始化失败", e);
                }
            }
            return fences;
        });
        LOGGER.info("查询电子围栏规则 耗时: {} ms", System.currentTimeMillis() - start);
        return result;
    }

    /**
     * 初始化电子围栏事件
     *
     * @param fenceId    围栏ID
     * @param fenceEvent 围栏事件
     * @param ruleType   1、驶离；2、驶入；3、驶入驶离
     * @return 电子围栏事件
     */
    @NotNull
    private ImmutableMap<String, EventCron> initFenceEvent(final String fenceId, final Map<String, Set<String>> fenceEvent, String ruleType) {
        ImmutableMap.Builder<String, EventCron> events = new ImmutableMap.Builder<>();
        switch (ruleType) {
            case "1":
                LOGGER.info("FENCE_ID:{} 规则类型[驶离]", fenceId);
                createOutsideEvent(fenceId, events, fenceEvent);
                break;
            case "2":
                LOGGER.info("FENCE_ID:{} 规则类型[驶入]", fenceId);
                createInsideEvent(fenceId, events, fenceEvent);
                break;
            case "3":
                LOGGER.info("FENCE_ID:{} 规则类型[驶入驶离]", fenceId);
                createInsideEvent(fenceId, events, fenceEvent);
                createOutsideEvent(fenceId, events, fenceEvent);
                break;
            default:
                LOGGER.info("FENCE_ID:{} 规则类型[全部]", fenceId);
                createInsideEvent(fenceId, events, fenceEvent);
                createOutsideEvent(fenceId, events, fenceEvent);
                break;
        }
        return events.build();
    }

    /**
     * 生成驶离事件
     *
     * @param fenceId    围栏ID
     * @param events     事件列表
     * @param fenceEvent 围栏与事件关系
     */
    private void createOutsideEvent(String fenceId, ImmutableMap.Builder<String, EventCron> events, Map<String, Set<String>> fenceEvent) {
        //驶离
        events.put(SysDefine.FENCE_OUTSIDE_EVENT_ID, new DriveOutside(SysDefine.FENCE_OUTSIDE_EVENT_ID, null));
        //生成围栏与事件关系
        Set<String> outSideEvent = fenceEvent.getOrDefault(fenceId, new HashSet<>());
        outSideEvent.add(SysDefine.FENCE_OUTSIDE_EVENT_ID);
        fenceEvent.put(fenceId, outSideEvent);
    }

    /**
     * 生成驶入事件
     *
     * @param fenceId    围栏ID
     * @param events     事件列表
     * @param fenceEvent 围栏与事件关系
     */
    private void createInsideEvent(String fenceId, ImmutableMap.Builder<String, EventCron> events, Map<String, Set<String>> fenceEvent) {
        //驶入
        events.put(SysDefine.FENCE_INSIDE_EVENT_ID, new DriveInside(SysDefine.FENCE_INSIDE_EVENT_ID, null));
        //生成围栏与事件关系
        Set<String> inSideEvent = fenceEvent.getOrDefault(fenceId, new HashSet<>());
        inSideEvent.add(SysDefine.FENCE_INSIDE_EVENT_ID);
        fenceEvent.put(fenceId, inSideEvent);
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
    private ImmutableList<Cron> initFenceCron(String fenceId, int periodType, String week, Date startDate, Date endDate, Time startTime, Time endTime) {
        ImmutableList.Builder<Cron> cronBuilder = new ImmutableList.Builder<>();
        long startTimeNumber = 0;
        long endTimeNumber = 0;
        switch (periodType) {
            case 1:
                //单次执行
                if (startDate == null || endDate == null) {
                    LOGGER.warn("FENCE_ID:{} 执行计划[ 单次执行 ], 开始日期(startDate)或结束日期(endDate)为空，已启用默认执行计划[ 每天24小时都生效 ]", fenceId);
                    break;
                }
                if (startTime == null || endTime == null) {
                    LOGGER.warn("FENCE_ID:{} 执行计划[ 单次执行 ], 开始时间段(startTime)或结束时间段(endTime)为空，已启用默认执行计划[ 24小时都生效 ]", fenceId);
                    cronBuilder.add(new DailyOnce(startDate.getTime(), endDate.getTime(), 0, 0));
                } else {
                    startTimeNumber = DateExtension.getMillisecondOfDay(DateUtils.setMilliseconds(startTime, 0).getTime());
                    endTimeNumber = DateExtension.getMillisecondOfDay(DateUtils.setMilliseconds(endTime, 0).getTime());
                    if (startTimeNumber != endTimeNumber) {
                        endTimeNumber += 1000;
                    }
                    cronBuilder.add(new DailyOnce(startDate.getTime(), endDate.getTime(), startTimeNumber, endTimeNumber));
                }
                break;
            case 2:
                if (startTime == null || endTime == null) {
                    LOGGER.warn("FENCE_ID:{} 执行计划[ 每周循环 ] 周[ " + week + " ]执行, 开始时间段(startTime)或结束时间段(endTime)为空，已启用默认执行计划[ 24小时都生效 ]", fenceId);
                    break;
                }
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
                startTimeNumber = DateExtension.getMillisecondOfDay(DateUtils.setMilliseconds(startTime, 0).getTime());
                endTimeNumber = DateExtension.getMillisecondOfDay(DateUtils.setMilliseconds(endTime, 0).getTime());
                if (startTimeNumber != endTimeNumber) {
                    endTimeNumber += 1000;
                }
                cronBuilder.add(new WeeklyCycle((byte) weekFlag, startTimeNumber, endTimeNumber));
                break;
            case 3:
                //每天执行
                if (startTime == null || endTime == null) {
                    LOGGER.warn("FENCE_ID:{} 执行计划[ 每天执行 ], 开始时间段(startTime)或结束时间段(endTime)为空，已启用默认执行计划[ 24小时都生效 ]", fenceId, startTime == null, endTime == null);
                    break;
                }
                startTimeNumber = DateExtension.getMillisecondOfDay(DateUtils.setMilliseconds(startTime, 0).getTime());
                endTimeNumber = DateExtension.getMillisecondOfDay(DateUtils.setMilliseconds(endTime, 0).getTime());
                if (startTimeNumber != endTimeNumber) {
                    endTimeNumber += 1000;
                }
                cronBuilder.add(new DailyCycle(startTimeNumber, endTimeNumber));
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
    @NotNull
    private ImmutableMap<String, AreaCron> initFenceArea(String chartType, String lonlatRange) {
        if (StringUtils.isEmpty(lonlatRange)) {
            return ImmutableMap.of();
        }
        ImmutableMap.Builder<String, AreaCron> areas = new ImmutableMap.Builder<>();
        String areaId = SysDefine.FENCE_AREA_ID;
        //坐标值按逗号拆分后,length为2
        int coordinateSize = 2;
        switch (chartType) {
            case "1":
                //圆形区域
                String[] splitArray = lonlatRange.split("[;:]");
                if (splitArray.length < coordinateSize) {
                    break;
                }
                String[] coordinateArray = splitArray[1].split(",");
                if (coordinateArray.length < coordinateSize) {
                    break;
                }
                areas.put(areaId, new Circle(areaId, new Coordinate(Double.valueOf(coordinateArray[0]), Double.valueOf(coordinateArray[1])), Double.valueOf(splitArray[0]) / COORDINATE_COEFFICIENT, null));
                break;
            case "2":
                //多边形区域
                final Coordinate[] first = {null};
                final Coordinate[] last = {null};
                ImmutableList.Builder<Coordinate> coordinates = new ImmutableList.Builder();
                Arrays.stream(lonlatRange.split("[;:]")).forEach(item -> {
                    String[] coordinateArrays = item.split(",");
                    if (coordinateArrays.length < coordinateSize) {
                        return;
                    }
                    Coordinate point = new Coordinate(Double.valueOf(coordinateArrays[0]), Double.valueOf(coordinateArrays[1]));
                    if (first[0] == null) {
                        first[0] = point;
                    }
                    last[0] = point;
                    coordinates.add(point);
                });
                if (!first[0].equals(last[0])) {
                    //多边形没有闭环,添加坐标点使多边形闭合
                    coordinates.add(first[0]);
                }
                areas.put(areaId, new Polygon(areaId, coordinates.build(), null));
                break;
            default:
                break;
        }
        return areas.build();
    }

    /**
     * 电子围栏与车辆关联表
     */
    private static class FenceVehicle {
        private String vid;
        private String fenceId;

        public FenceVehicle(final String vid, final String fenceId) {
            this.vid = vid;
            this.fenceId = fenceId;
        }

        public String getVid() {
            return vid;
        }

        public String getFenceId() {
            return fenceId;
        }
    }

}
