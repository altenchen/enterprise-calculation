package storm.handler.cusmade;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 此类是所有报文判断产生结果的父接口
 * 对于不同企业的定制可以通过实现类可以采用调用链模式来调用
 * 后续可以配置在配置文件中，不同的企业不同的功能完全通过配置文件来定义
 * </p>
 * @author 76304
 *
 */
public interface InfoNotice {

	Map<String, Object> genotice(Map<String, String> dat);

	/**
	 * 生成通知
	 * @param dat
	 * @return 通知列表
	 */
	@NotNull
	List<Map<String, Object>> generateNotices(@NotNull Map<String, String> dat);

	List<Map<String, Object>> offlineMethod(long now);
}
