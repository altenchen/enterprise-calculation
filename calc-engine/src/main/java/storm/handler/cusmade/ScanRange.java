package storm.handler.cusmade;

/**
 * 扫描范围
 */
public enum ScanRange {

    /**
     * 全量数据
     */
    AllData(0),

    /**
     * 活跃数据
     */
    AliveData(1),

    /**
     * 其他定义
     */
    Otherwish(2);

    private final int value;
    private ScanRange(int value) {
        this.value = value;
    }
}
