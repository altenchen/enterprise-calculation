package storm.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.function.FunctionE;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author: xzp
 * @date: 2018-09-19
 * @description: 数据库查询工具类, 采用数据库连接池, 并用函数式接口封装, 无需关心资源释放.
 *
 * 单例模式, 每个 storm worker 仅一个实例.
 */
public final class SqlUtils {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(SqlUtils.class);

    private static HikariDataSource dataSource;

    static {
        // https://github.com/brettwooldridge/HikariCP

        final HikariConfig config = new HikariConfig();
        config.setDriverClassName(ConfigUtils.getSysDefine().getJdbcDriver());
        config.setJdbcUrl(ConfigUtils.getSysDefine().getJdbcUrl());
        config.setUsername(ConfigUtils.getSysDefine().getJdbcUsername());
        config.setPassword(ConfigUtils.getSysDefine().getJdbcPassword());

        // https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration

        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("useLocalSessionState", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("cacheResultSetMetadata", "true");
        config.addDataSourceProperty("cacheServerConfiguration", "true");
        config.addDataSourceProperty("elideSetAutoCommits", "true");
        config.addDataSourceProperty("maintainTimeStats", "false");
        dataSource = new HikariDataSource(config);
    }

    @NotNull
    private static final SqlUtils SINGLETON = new SqlUtils();

    @Contract(pure = true)
    public static SqlUtils getInstance() {
        return SINGLETON;
    }

    private SqlUtils() {
    }

    @Nullable
    public <R> R query(
        @NotNull final String sql,
        @NotNull final FunctionE<@NotNull ? super ResultSet, @Nullable ? extends R, ? extends SQLException> processSet) {

        if (StringUtils.isBlank(sql)) {
            LOG.warn("SQL查询语句为空", new IllegalArgumentException("sql"));
            return null;
        }

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            connection = dataSource.getConnection();
            if (null == connection) {
                LOG.warn("创建数据库连接失败.");
                return null;
            }

            if(connection.isClosed()) {
                LOG.warn("数据库连接已关闭");
                return null;
            }

            statement = connection.createStatement();
            if (null == statement) {
                LOG.warn("创建数据库查询声明失败.");
                return null;
            }

            resultSet = statement.executeQuery(sql);

            return processSet.apply(resultSet);

        } catch (final SQLException e) {
            LOG.warn("执行数据库操作异常", e);
            return null;
        } finally {
            close(resultSet, statement, connection);
        }
    }

    /**
     * 释放系统资源
     */
    private static void close(
        @Nullable final ResultSet resultSet,
        @Nullable final Statement statement,
        @Nullable final Connection connection) {

        try {
            if(null != resultSet) {
                resultSet.close();
            }
        } catch (SQLException e) {
            LOG.warn("释放数据库查询结果集资源异常.", e);
        } finally {
            try {
                if(null != statement) {
                    statement.close();
                }
            } catch (SQLException e) {
                LOG.warn("释放数据库查询声明资源异常.", e);
            } finally {
                try {
                    if(null != connection) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    LOG.warn("释放数据库连接资源异常.", e);
                }
            }
        }
    }
}
