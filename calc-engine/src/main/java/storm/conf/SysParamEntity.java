package storm.conf;

public class SysParamEntity {

    /**
     * 充电车类型Id集合, 用英文逗号分隔. more params spit with ,
     */
    private String chargeCarTypeId = "402894605f511508015f516968890198";
    /**
     * 按字节解析的故障码
     */
    private String alarmCodeSql = "SELECT faultCode.id fault_id,faultCode.fault_type,faultCode.analyze_type,GROUP_CONCAT(model.id) model_num,'0' AS exception_type,faultCode.id AS exception_id,faultCode.normal_code exception_code,'0' AS response_level FROM sys_fault_code faultCode LEFT JOIN sys_fault_code_model code_model ON faultCode.id=code_model.fault_code_id LEFT JOIN sys_veh_model model ON model.id=code_model.veh_model_id WHERE faultCode.is_delete='0' AND faultCode.analyze_type='1' GROUP BY faultCode.id UNION ALL SELECT faultCode.id fault_id,faultCode.fault_type,faultCode.analyze_type,GROUP_CONCAT(model.id) model_num,'1' AS exception_type,excep.id AS exception_id,excep.exception_code exception_code,excep.response_level AS response_level FROM sys_fault_code_exception excep LEFT JOIN sys_fault_code faultCode ON excep.fault_code_id=faultCode.id LEFT JOIN sys_fault_code_model code_model ON faultCode.id=code_model.fault_code_id LEFT JOIN sys_veh_model model ON model.id=code_model.veh_model_id WHERE excep.is_delete='0' AND faultCode.is_delete='0' AND faultCode.analyze_type='1' AND excep.is_create_fault_info = 1 GROUP BY excep.id";
    /**
     * 按位解析的故障码
     */
    private String alarmCodeBitSql = "SELECT faultCode.id fault_id,faultCode.fault_type,faultCode.analyze_type,faultCode.param_length,GROUP_CONCAT(model.id) model_num,excep.start_point,excep.id exception_id,excep.exception_code,faultCode.threshold time_threshold,excep.response_level FROM sys_fault_code_exception excep LEFT JOIN sys_fault_code faultCode ON excep.fault_code_id=faultCode.id LEFT JOIN sys_fault_code_model code_model ON faultCode.id=code_model.fault_code_id LEFT JOIN sys_veh_model model ON model.id=code_model.veh_model_id WHERE excep.is_delete='0' AND faultCode.is_delete='0' AND faultCode.analyze_type='2' AND excep.is_create_fault_info = 1 GROUP BY excep.id";
    /**
     * 车辆车型表
     */
    private String vehModelSql = "SELECT veh.uuid vid,model.id mid FROM sys_vehicle veh LEFT JOIN sys_veh_model model ON veh.veh_model_id=model.id WHERE model.id IS NOT NULL";
    /**
     * 电子围栏
     */
    private String fenceSql = new StringBuilder()
        .append(" SELECT SEF.ID AS FENCE_ID, SEF.RULE_TYPE, SEF.PERIOD_TYPE, SEF.START_DATE, SEF.END_DATE, SEF.WEEK, SEF.START_TIME, SEF.END_TIME, SEF.CHART_TYPE, SEF.LONLAT_RANGE ")
        .append(" FROM SYS_ELECTRONIC_FENCE SEF ")
        .append(" WHERE SEF.STATUS_FLAG = 1 ")
        .append(" AND SEF.RULE_STATUS = 1 ")
        .toString();
    /**
     * 电子围栏与车辆关联
     */
    private String fenceVehicleSql = "SELECT LK.ELECTRONIC_FENCE_ID AS FENCE_ID, LK.VEH_ID AS VID FROM SYS_FENCE_VEH_LK LK";
    /**
     * 预警规则(旧)
     */
    private String earlyWarningSql = "SELECT ID, NAME, VEH_MODEL_ID, LEVELS, DEPEND_ID, L1_SEQ_NO, EXPR_LEFT, L2_SEQ_NO, EXPR_MID, R1_VAL, R2_VAL FROM SYS_DATA_CONST WHERE TYPE = 1 AND IS_VALID = 1 AND ID is not null AND R1_VAL is not null";
    /**
     * 偏移系数自定义数据项
     */
    private String itemCoefOffsetSql = "SELECT SEQ_NO,IS_ARRAY,FACTOR,OFFSET,IS_CUSTOM FROM SYS_DATA_ITEM WHERE IS_VALID = 1 AND SEQ_NO IS NOT NULL AND (OFFSET is not null OR FACTOR is not null)";
    /**
     * 预警规则
     */
    private String alarmRuleSql = "select id,name,l1_seq_no,is_last1,l2_seq_no,is_last2,expr_left,r1_val,r2_val,expr_mid,levels,ifnull(veh_model_id,'ALL') from sys_data_const where is_valid=1 and type=1 and (depend_id is null or depend_id = '')";
    /**
     * 偏移系数规则
     */
    private String dataOffsetCoefficientSql = "select id,seq_no,ifnull(offset,0),ifnull(factor,1),ifnull(dot,16),ifnull(item_type,2),ifnull(veh_model,'Default') from sys_data_item where is_valid=1 and id is not null and seq_no is not null and is_array=0 and (item_type is null or item_type=1 or item_type=2)";

    private String vehicleIdSql = "select uuid from sys_vehicle where uuid is not null";

    public String getChargeCarTypeId() {
        return chargeCarTypeId;
    }

    public void setChargeCarTypeId(final String chargeCarTypeId) {
        this.chargeCarTypeId = chargeCarTypeId;
    }

    public String getAlarmCodeSql() {
        return alarmCodeSql;
    }

    public void setAlarmCodeSql(final String alarmCodeSql) {
        this.alarmCodeSql = alarmCodeSql;
    }

    public String getAlarmCodeBitSql() {
        return alarmCodeBitSql;
    }

    public void setAlarmCodeBitSql(final String alarmCodeBitSql) {
        this.alarmCodeBitSql = alarmCodeBitSql;
    }

    public String getVehModelSql() {
        return vehModelSql;
    }

    public void setVehModelSql(final String vehModelSql) {
        this.vehModelSql = vehModelSql;
    }

    public String getFenceSql() {
        return fenceSql;
    }

    public void setFenceSql(final String fenceSql) {
        this.fenceSql = fenceSql;
    }

    public String getFenceVehicleSql() {
        return fenceVehicleSql;
    }

    public void setFenceVehicleSql(final String fenceVehicleSql) {
        this.fenceVehicleSql = fenceVehicleSql;
    }

    public String getEarlyWarningSql() {
        return earlyWarningSql;
    }

    public void setEarlyWarningSql(final String earlyWarningSql) {
        this.earlyWarningSql = earlyWarningSql;
    }

    public String getItemCoefOffsetSql() {
        return itemCoefOffsetSql;
    }

    public void setItemCoefOffsetSql(final String itemCoefOffsetSql) {
        this.itemCoefOffsetSql = itemCoefOffsetSql;
    }

    public String getAlarmRuleSql() {
        return alarmRuleSql;
    }

    public void setAlarmRuleSql(final String alarmRuleSql) {
        this.alarmRuleSql = alarmRuleSql;
    }

    public String getDataOffsetCoefficientSql() {
        return dataOffsetCoefficientSql;
    }

    public void setDataOffsetCoefficientSql(final String dataOffsetCoefficientSql) {
        this.dataOffsetCoefficientSql = dataOffsetCoefficientSql;
    }

    public String getVehicleIdSql() {
        return vehicleIdSql;
    }

    public void setVehicleIdSql(final String vehicleIdSql) {
        this.vehicleIdSql = vehicleIdSql;
    }
}
