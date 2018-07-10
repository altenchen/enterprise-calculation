package storm.cache.util;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import storm.dao.DataToRedis;

public class RedisOrganizationUtil {

    private static DataToRedis redis;
    private static Map<String, Set<String>> districtIds;
    private static Map<String, Set<String>>carUser;
    static{
        redis=new DataToRedis();
        districtIds=getDistrictS();
        carUser=getCarUser();
    }

    private static Map<String, Set<String>> getCarUser(){

        Map<String, Set<String>>userVids=getUserCars();
        Map<String, Set<String>>carUser=getCarUser(userVids);
        return carUser;
    }

    public static Map<String, Set<String>> getCarUser(boolean b){

        if(b){
            carUser=getCarUser();
        }
        return carUser;
    }

    private static Map<String, Set<String>> getCarUser(Map<String, Set<String>>userVids){

        Map<String, Set<String>> carUser=new HashMap<String, Set<String>>();
        for (Map.Entry<String, Set<String>> entry : userVids.entrySet()) {
            String user=entry.getKey();
            Set<String> vids=entry.getValue();

            for (String vin : vids) {
                int pos=vin.indexOf(",");
                if(-1 == pos)
                    continue;
                vin=vin.substring(0,pos);
                Set<String> userIds=carUser.get(vin);
                if(null==userIds)
                    userIds=new HashSet<String>();
                userIds.add(user);
                carUser.put(vin, userIds);
//                String vid=vin.substring(pos+1);
            }
        }

        return carUser;
    }

    private static Map<String, Set<String>> getUserCars(){

        Map<String, Set<String>>userGroups=new HashMap<String,Set<String>>();
        Map<String, Set<String>>groupVids=new HashMap<String, Set<String>>();
        Map<String, Set<String>>userVids=new HashMap<String, Set<String>>();

        Map<String,String> userMap=redis.getMap(10, "XNY.USERINFO");
        if(null==userMap || userMap.size()==0)
            return userVids;
        Set<String>userIds=userMap.keySet();

        Set<String> groupIds=redis.getKeysSet(10, "XNY.GROUP.USER.*");
        for (String string : groupIds) {

            String groupId=string.substring(string.lastIndexOf("."));
            Set<String>groupUserIds=redis.getSmembersSet(10, string);

            for (String user : groupUserIds) {
                Set<String> gids=userGroups.get(user);
                if (null==gids)
                    gids=new HashSet<String>();
                gids.add(groupId);
                userGroups.put(user, gids);
            }

            Set<String>groupCars=redis.getSmembersSet(10, "XNY.GROUP.VEH."+groupId);
            groupVids.put(groupId, groupCars);
        }

        for (String userId : userIds) {
            Set<String> vids=redis.getSmembersSet(10, "XNY.USER.CAR."+userId);
            userVids.put(userId, vids);
        }

        for (String userId : userIds) {
            Set<String> vids=userVids.get(userId);
            if (null==vids)
                vids=new HashSet<String>();
            Set<String> gids=userGroups.get(userId);
            if(null !=gids)
            for (String group : gids) {
                Set<String>vidSet=groupVids.get(group);
                if(null!=vidSet)
                    vids.addAll(vidSet);
            }
            userVids.put(userId, vids);
        }

        return userVids;
    }

    private static Map<String, Set<String>> getDistrictCars(){
        Map<String, Set<String>> districtMap=new HashMap<String,Set<String>>();

        Set<String> districtOrg=redis.getKeysSet(10, "XNY.AREA.*");
        if(!CollectionUtils.isEmpty(districtOrg))
        for (String org : districtOrg) {
            int last=org.lastIndexOf(".");
            if(last+1==org.length())
                continue;
            String districtId=org.substring(last+1);
            if(StringUtils.isEmpty(districtId))
                continue;
            Set<String> sets=districtMap.get(districtId);
            if(null==sets)
                sets=new HashSet<String>();

            sets.add(districtId);
            Set<String>districtIds=redis.getSmembersSet(10, org);
            if (null==districtIds || districtIds.size()==0)
                continue;
            sets.addAll(districtIds);

            for (String pid : districtIds) {
                Set<String>ids=redis.getSmembersSet(10, "XNY.AREA."+pid);
                if (null==ids || ids.size()==0)
                    continue;
                sets.addAll(ids);

                for (String id : ids) {
                    Set<String>idCs=redis.getSmembersSet(10, "XNY.AREA."+id);
                    if(null==idCs || idCs.size()==0)
                        continue;
                    sets.addAll(idCs);
                    for (String idC : idCs) {
                        Set<String>idXs=redis.getSmembersSet(10, "XNY.AREA."+idC);
                        if(null==idXs || idXs.size()==0)
                            continue;
                        sets.addAll(idXs);
                        for (String idX : idXs) {
                            Set<String>idZs=redis.getSmembersSet(10, "XNY.AREA."+idX);
                            if(null==idZs || idZs.size()==0)
                                continue;
                            sets.addAll(idZs);
                        }
                    }

                }
            }
            districtMap.put(districtId, sets);
        }
        return districtMap;
    }

    public static Map<String, Set<String>> getDistrictClassify(){
        if(null == districtIds)
            districtIds=getDistrictS();
        return districtIds;
    }

    private static Map<String, Set<String>> getDistrictS(){
        Map<String, Set<String>> districtMap=getDistrictCars();
        Map<String, Set<String>> districtIds=new HashMap<String, Set<String>>();

        for (Map.Entry<String, Set<String>> entry : districtMap.entrySet()) {
            String districtId=entry.getKey();
            Set<String> districts=entry.getValue();
            if(StringUtils.isEmpty(districtId)
                    || CollectionUtils.isEmpty(districts))
                continue;
            for (String district : districts) {
                Set<String> pids=districtIds.get(district);
                if(null==pids)
                    pids=new HashSet<String>();

                pids.add(district);
                pids.add(districtId);
                districtIds.put(district, pids);
            }
        }

        return districtIds;
    }

    public static void main(String[] args) {

    }

    public static void init() {

    }

}
