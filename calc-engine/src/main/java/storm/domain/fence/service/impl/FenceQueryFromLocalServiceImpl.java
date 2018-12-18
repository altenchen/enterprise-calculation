package storm.domain.fence.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dao.DataToRedis;
import storm.domain.fence.Fence;
import storm.domain.fence.area.AreaCron;
import storm.domain.fence.area.Circle;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.area.Polygon;
import storm.domain.fence.cron.Cron;
import storm.domain.fence.cron.DailyOnce;
import storm.domain.fence.cron.WeeklyCycle;
import storm.domain.fence.event.DriveInside;
import storm.domain.fence.event.DriveOutside;
import storm.domain.fence.event.EventCron;
import storm.system.SysDefine;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 模拟数据-用于数据库尚没有数据的时候自测使用
 *
 * @author 智杰
 */
@SuppressWarnings("Duplicates")
public class FenceQueryFromLocalServiceImpl extends AbstractFenceQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(FenceQueryFromLocalServiceImpl.class);

    private Date prevDate;
    private Date nextDate;
    private Time startTime;
    private Time endTime;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

    /**
     * 围栏ID - 写死
     */
    private List<String> fenceIds = new ArrayList<String>(6) {{
        add("0bc51681-5de5-42d6-af2e-62c56424d395");
    }};
    /**
     * 车辆VID - 写死, 测试的时候修改成自己的车辆VID即可
     */
    private List<String> vids = new ArrayList<String>(6) {{
        add("618d4b48-cfdc-42d5-a5be-bcfbb210e0e6");
    }};

    public FenceQueryFromLocalServiceImpl() {
        super(new DataToRedis());
        try {
            Calendar calendar = Calendar.getInstance();
            //昨天
            calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) - 2);
            String prevDateString = dateFormat.format(calendar.getTime());
            prevDate = dateFormat.parse(prevDateString);

            //明天
            calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + 4);
            String nextDateString = dateFormat.format(calendar.getTime());
            nextDate = dateFormat.parse(nextDateString);

            //09:33:00
//            startTime = new Time(34380000);
//            startTime = new Time(count(9,33,00));
            startTime = new Time(9,33,00);
            //22:59:59
//            endTime = new Time(count(22,59,59));
            endTime = new Time(22,59,59);
        } catch (ParseException e) {
            LOGGER.info("日期初始化异常", e);
        }
    }

    private static final long hourTime = 3600000;
    private static final long minTime = 60000;
    private static final long secTime = 1000;
    private long count(int hour, int min, int sec){
        return hour * hourTime + minTime * min + sec * secTime;
    }

    @Override
    protected void dataQuery(final DataInitCallback dataInitCallback) {
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

        //生成围栏与车辆关系
        fences.forEach(fence -> {
            vids.forEach(vid -> {
                ImmutableMap.Builder<String, Fence> builder = vehicleFenceRuleTemp.getOrDefault(vid, new ImmutableMap.Builder<>()).put(fence.getFenceId(), fence);
                vehicleFenceRuleTemp.put(vid, builder);

                Set<String> vids = fenceVehicle.getOrDefault(fence.getFenceId(), new HashSet<>());
                vids.add(vid);
                fenceVehicle.put(fence.getFenceId(), vids);
            });
            fenceMap.put(fence.getFenceId(), fence);

        });
        //转成ImmutableMap
        vehicleFenceRuleTemp.forEach((key, value) -> vehicleFenceRule.put(key, value.build()));

        //完成初始化
        dataInitCallback.finishInit(vehicleFenceRule, fenceEvent, fenceVehicle, fenceMap);
        LOGGER.info("初始化车辆围栏缓存 耗时: {} ms. 关联围栏的车辆数: {}. 围栏数: {}", System.currentTimeMillis() - start, vehicleFenceRule.size(), fences.size());

    }

    private List<Fence> queryFences(final Map<String, Set<String>> fenceEvent) {
        long start = System.currentTimeMillis();
        LOGGER.info("查询电子围栏规则");
        List<Fence> result = new ArrayList<>(6);
        final int[] index = {0};
        fenceIds.forEach(fenceId -> {
            ImmutableMap<String, EventCron> events;
            ImmutableList<Cron> crons;
            if (index[0] == 0) {
                //驶入驶离
//                events = ImmutableMap.of(SysDefine.FENCE_OUTSIDE_EVENT_ID, new DriveOutside(SysDefine.FENCE_OUTSIDE_EVENT_ID, null), SysDefine.FENCE_INSIDE_EVENT_ID, new DriveInside(SysDefine.FENCE_INSIDE_EVENT_ID, null));
                //驶入
                events = ImmutableMap.of(SysDefine.FENCE_INSIDE_EVENT_ID, new DriveInside(SysDefine.FENCE_INSIDE_EVENT_ID, null));
                //驶离
//                events = ImmutableMap.of(SysDefine.FENCE_OUTSIDE_EVENT_ID, new DriveOutside(SysDefine.FENCE_OUTSIDE_EVENT_ID, null));

                //单次执行
                crons = ImmutableList.of(new DailyOnce(prevDate.getTime(), nextDate.getTime(), startTime.getTime(), endTime.getTime()));

                Set<String> event = fenceEvent.getOrDefault(fenceId, new HashSet<>());
                event.add(SysDefine.FENCE_OUTSIDE_EVENT_ID);
                event.add(SysDefine.FENCE_INSIDE_EVENT_ID);
                fenceEvent.put(fenceId, event);
            } else {
                events = ImmutableMap.of(SysDefine.FENCE_INSIDE_EVENT_ID, new DriveInside(SysDefine.FENCE_INSIDE_EVENT_ID, null));
                //周一， 周三， 周五执行
                byte weekFlag = 1 << (1 % 7) | 1 << (3 % 7) | 1 << (5 % 7);
                crons = ImmutableList.of(new WeeklyCycle(weekFlag, startTime.getTime(), endTime.getTime()));

                Set<String> event = fenceEvent.getOrDefault(fenceId, new HashSet<>());
                event.add(SysDefine.FENCE_INSIDE_EVENT_ID);
                fenceEvent.put(fenceId, event);
            }

            Fence fence;
            //港湾大道 - 珠海市社会保险基金管理中心高新办事处
            if (index[0] == 0) {
                //多边形围栏
                fence = new Fence(fenceId, ImmutableMap.of(SysDefine.FENCE_AREA_ID, initArea(2, "113.581,22.375365;113.600999,22.377269;113.611384,22.364808;113.590957,22.35441;113.574992,22.36203")), events, crons);
            } else {
                //圆形围栏
                fence = new Fence(fenceId, ImmutableMap.of(SysDefine.FENCE_AREA_ID, initArea(1, "1320;113.59724,22.36536")), events, crons);
            }
            result.add(fence);
            index[0]++;
        });
        LOGGER.info("查询电子围栏规则 耗时: {} ms", System.currentTimeMillis() - start);
        return result;
    }

    public AreaCron initArea(int chartType, String lonlatRange) {
        if (StringUtils.isEmpty(lonlatRange)) {
            return null;
        }
        AreaCron area = null;
        switch (chartType) {
            case 1:
                //圆形区域
                String[] splitArray = lonlatRange.split(";");
                if (splitArray.length < 2) {
                    break;
                }
                String[] coordinateArray = splitArray[1].split(",");
                if (coordinateArray.length < 2) {
                    break;
                }
                area = new Circle(SysDefine.FENCE_AREA_ID, new Coordinate(Double.valueOf(coordinateArray[0]), Double.valueOf(coordinateArray[1])), Double.valueOf(splitArray[0]), null);
                break;
            case 2:
                final Coordinate[] first = {null};
                final Coordinate[] last = {null};
                ImmutableList.Builder<Coordinate> coordinates = new ImmutableList.Builder();
                Arrays.stream(lonlatRange.split(";")).forEach(item -> {
                    String[] coordinateArrays = item.split(",");
                    if (coordinateArrays.length < 2) {
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
                area = new Polygon(SysDefine.FENCE_AREA_ID, coordinates.build(), null);
                break;
            default:
                break;
        }
        return area;
    }

}
