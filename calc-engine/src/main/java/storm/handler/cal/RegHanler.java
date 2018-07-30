package storm.handler.cal;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.sun.jersey.core.util.Base64;

import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.protocol.SUBMIT_LOGIN;
import storm.system.DataKey;
import storm.system.ProtocolItem;
import storm.system.SysDefine;

class RegHanler {

    /**
     * vid ,isreg
     */
    private Map<String, Boolean> regVids = new HashMap<>();

    Map<String, Object> regHandler(Map<String, String> msg) {
        if (null == msg || msg.size() < 1) {
            return null;
        }

        // 平台注册通知类型 0:从未上过线，1:车机终端上线 ，2:车机离线，3:平台上线，4:平台下线
        final String type = msg.get(ProtocolItem.REG_TYPE);
        String time = msg.get(SysDefine.TIME);

        try {
            if ("1".equals(type)) {//车辆上线
                String vid = msg.get(DataKey.VEHICLE_ID);
                if (vid == null || vid.equals("")) {
                    System.out.println("Login Error VEHICLE_ID: ");
                    return null;
                }

                String iccid = null;
                String icc = msg.get(ProtocolItem.ICCID);
                if (!StringUtils.isEmpty(icc)) {

                    iccid = new String(Base64.decode(icc), "GBK");
                }
                if (iccid == null || "".equals(iccid.trim())) {
                    icc = msg.get(SUBMIT_LOGIN.ICCID_ITEM);
                    if (!StringUtils.isEmpty(icc)) {
                        iccid = new String(Base64.decode(icc), "GBK");
                    }
                }
                if (iccid == null || "".equals(iccid.trim())) {
                    iccid = "";
                }

                Map<String, Object> esmap = new TreeMap<>();
                esmap.put(EsField.vid, vid);
                esmap.put(EsField.terminalTime, toTimeString(time));
                if (!"".equals(iccid)) {
                    iccid = iccid.trim();
                    int end = iccid.indexOf("\\0", 0);
                    if (end != -1) {
                        iccid = iccid.substring(0, end);
                    }
                    if (!"".equals(iccid)) {
                        esmap.put(EsField.iccid, iccid);
                    }
                }

                if (regVids.containsKey(vid)) {
                    esmap.put(EsField.carStatus, 1);
                } else {

                    if ("0".equals(msg.get(ProtocolItem.REG_STATUS))) {
                        regVids.put(vid, true);
                        esmap.put(EsField.monitor, 1);
                        esmap.put(EsField.status, 0);
                        esmap.put(EsField.carStatus, 1);
                    }
                }
                return esmap;
            } else if ("2".equals(type)) {//车辆离线
                String vid = msg.get(DataKey.VEHICLE_ID);
                if (vid == null || vid.equals("")) {
                    System.out.println("Login Error VID: ");
                    return null;
                }

                Map<String, Object> esmap = new TreeMap<String, Object>();
                regVids.put(vid, false);
                esmap.put(EsField.vid, vid);
                esmap.put(EsField.terminalTime, toTimeString(time));
                esmap.put(EsField.carStatus, 0);
                esmap.put(EsField.onlineStatus, 0);
                //esmap.put(EsField.monitor, 1);
                return esmap;

            } else if ("0".equals(type)) {
            } else if ("3".equals(type) || "4".equals(type)) {
            } else {
                System.out.println("ERROR, TYPE is null OR others------------ TYPE: " + type);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * yyyyMMddHHmmss -> yyyy-MM-dd HH:mm:ss
     */
    @Nullable
    private String toTimeString(@NotNull final String time) {
        if (null == time || time.length() != 14) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        builder.append(time, 0, 4).append("-")
            .append(time, 4, 6).append("-")
            .append(time, 6, 8).append(" ")
            .append(time, 8, 10).append(":")
            .append(time, 10, 12).append(":")
            .append(time, 12, 14);
        return builder.toString();
    }
}
