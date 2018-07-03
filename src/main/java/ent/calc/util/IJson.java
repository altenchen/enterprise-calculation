package ent.calc.util;

import com.alibaba.fastjson.JSON;

/**
 * @author: xzp
 * @date: 2018-07-03
 * @description:
 */
public interface IJson {

    default String toJson(Object object) {
        return JSON.toJSONString(object);
    }
}
