package storm.domain.fence.data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import storm.domain.fence.service.impl.AbstractFenceQuery;
import storm.system.SysDefine;

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
    private Date startTime;
    private Date endTime;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

    /**
     * 围栏ID - 写死
     */
    private List<String> fenceIds = new ArrayList<String>(6) {{
        add("0bc51681-5de5-42d6-af2e-62c56424d395");
        add("8c451488-a6a2-496f-8f92-584db1bb681f");
        add("a7d1ac74-a6ca-4664-b6d9-fcab6d0c5c63");
        add("13aa3ca0-d517-471c-94bc-272fe20c655b");
        add("5bdbd028-6927-4fc5-af6e-1c48150090ba");
    }};
    /**
     * 车辆VID - 写死, 测试的时候修改成自己的车辆VID即可
     */
    private List<String> vids = new ArrayList<String>(6) {{
        add("9ebb93af-faf9-46c3-a637-9c46b0e61256");
        add("68145d1c091f44eebff42263bd16ac02");
    }};

    public FenceQueryFromLocalServiceImpl() {
        try {
            Calendar calendar = Calendar.getInstance();
            //昨天
            calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) - 1);
            String prevDateString = dateFormat.format(calendar.getTime());
            prevDate = dateFormat.parse(prevDateString);

            //明天
            calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + 2);
            String nextDateString = dateFormat.format(calendar.getTime());
            nextDate = dateFormat.parse(nextDateString);

            startTime = timeFormat.parse("00:00:00");
            endTime = timeFormat.parse("23:59:59");
        } catch (ParseException e) {
            LOGGER.info("日期初始化异常", e);
        }
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
        });
        //转成ImmutableMap
        vehicleFenceRuleTemp.forEach((key, value) -> {
            vehicleFenceRule.put(key, value.build());
        });

        //完成初始化
        dataInitCallback.finishInit(vehicleFenceRule, fenceEvent, fenceVehicle);
        LOGGER.info("初始化车辆围栏缓存 耗时: {} ms. 关联围栏的车辆数: {}. 围栏数: {}", System.currentTimeMillis() - start, vehicleFenceRule.size(), fences.size());

    }

    private List<Fence> queryFences(final Map<String, Set<String>> fenceEvent) {
        long start = System.currentTimeMillis();
        LOGGER.info("查询电子围栏规则");
        List<Fence> result = new ArrayList<>(6);
        final int[] index = {0};
        fenceIds.forEach(fenceId -> {
            ImmutableMap<String, EventCron> events;
            ImmutableList<Cron> crons = null;
            if (index[0] % 3 == 0) {
                events = ImmutableMap.of(SysDefine.FENCE_OUTSIDE_EVENT_ID, new DriveInside(SysDefine.FENCE_OUTSIDE_EVENT_ID, null));
                //单次执行
                crons = ImmutableList.of(new DailyOnce(prevDate.getTime(), nextDate.getTime(), startTime.getTime(), endTime.getTime()));
            } else if (index[0] % 2 == 0) {
                events = ImmutableMap.of(SysDefine.FENCE_INSIDE_EVENT_ID, new DriveOutside(SysDefine.FENCE_INSIDE_EVENT_ID, null));
                //周一， 周三， 周五执行
                byte weekFlag = 1 << (1 % 7) | 1 << (3 % 7) | 1 << (5 % 7);
                crons = ImmutableList.of(new WeeklyCycle(weekFlag, startTime.getTime(), endTime.getTime()));
            } else {
                events = ImmutableMap.of(SysDefine.FENCE_OUTSIDE_EVENT_ID, new DriveInside(SysDefine.FENCE_OUTSIDE_EVENT_ID, null), SysDefine.FENCE_INSIDE_EVENT_ID, new DriveOutside(SysDefine.FENCE_INSIDE_EVENT_ID, null));
                //每天执行
                crons = ImmutableList.of(new DailyCycle(startTime.getTime(), endTime.getTime()));
            }

            Fence fence = null;
            if (index[0] % 2 == 0) {
                fence = new Fence(fenceId, ImmutableMap.of(SysDefine.FENCE_INSIDE_EVENT_ID, initArea(1, "3636;116.321307,39.919237")), events, crons);
            } else {
                fence = new Fence(fenceId, ImmutableMap.of(SysDefine.FENCE_INSIDE_EVENT_ID, initArea(2, "116.351519,39.901856;116.389971,39.928189;116.459323,39.909757;116.456576,39.864973;116.33916,39.818578;116.311694,39.866027;116.351519,39.901856")), events, crons);
            }
            result.add(fence);
            index[0]++;
        });
        LOGGER.info("查询电子围栏规则 耗时: {} ms", System.currentTimeMillis() - start);
        return result;
    }

    public AreaCron initArea(int chartType, String lonlatRange) {
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
                ImmutableList.Builder<Coordinate> coordinates = new ImmutableList.Builder();
                Arrays.stream(lonlatRange.split(";")).forEach(item -> {
                    String[] coordinateArrays = item.split(",");
                    if (coordinateArrays.length < 2) {
                        return;
                    }
                    coordinates.add(new Coordinate(Double.valueOf(coordinateArrays[0]), Double.valueOf(coordinateArrays[1])));
                });
                area = new Polygon(SysDefine.FENCE_AREA_ID, coordinates.build(), null);
                break;
            default:
                break;
        }
        return area;
    }


    public static void main(String[] args) {

        IFenceQueryService service = new FenceQueryFromLocalServiceImpl();
        ImmutableMap<String, Fence> data = service.query("68145d1c091f44eebff42263bd16ac02");
        System.out.println("==");

    }
}
