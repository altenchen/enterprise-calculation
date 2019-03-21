package storm;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import storm.dto.notice.VehicleNotice;

import java.util.UUID;

@DisplayName("beanUtil测试")
public class BeanUtilsCopyTest {

    @Test
    public void copy(){
        VehicleNotice n1 = new VehicleNotice();
        n1.setVid(UUID.randomUUID().toString());
        n1.setLocation("100,100");
        System.out.println(n1.getVid());

        VehicleNotice n2 = new VehicleNotice();
        n2.setVid(UUID.randomUUID().toString());
        n2.setStatus("1");
        System.out.println(n2.getVid() + " " + n2.getStatus());

        BeanUtil.copyProperties(n1, n2, CopyOptions.create().setIgnoreNullValue(true));

        System.out.println("=========");
        System.out.println(JSON.toJSONString(n1));
        System.out.println(JSON.toJSONString(n2));
    }

    @Test
    public void testUUID() {
        String s = UUID.randomUUID().toString();
        System.out.println(s);
    }

}
