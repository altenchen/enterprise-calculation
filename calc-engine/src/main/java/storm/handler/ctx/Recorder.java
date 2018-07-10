package storm.handler.ctx;

import java.util.Map;

/**
 * Redis记录器接口
 */
public interface Recorder {

    /**
     * 更新哈希表
     * @param dbIndex 数据库索引
     * @param type 表名
     * @param id 项ID
     * @param ctx 上下文
     */
    void save(
        int dbIndex,
        String type,
        String id,
        Map<String, Object>ctx);


    /**
     * 更新哈希表
     * @param dbIndex 数据库索引
     * @param type 表名
     * @param ctxs field-value pairs
     */
    void save(
        int dbIndex,
        String type,
        Map<String, Map<String, Object>> ctxs);


    /**
     * 删除哈希项
     * @param dbIndex 数据库索引
     * @param type key
     * @param ids fields
     */
    void del(int dbIndex, String type, String ... ids);

    /**
     * 从Redis拉取数据初始化initMap
     * @param dbIndex 数据库索引
     * @param type key
     * @param initMap
     */
    void rebootInit(int dbIndex, String type, Map<String, Map<String, Object>> initMap);
}
