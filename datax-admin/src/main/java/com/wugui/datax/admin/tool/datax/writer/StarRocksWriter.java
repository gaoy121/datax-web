package com.wugui.datax.admin.tool.datax.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.wugui.datatx.core.util.Constants;
import com.wugui.datax.admin.entity.JobDatasource;
import com.wugui.datax.admin.tool.pojo.DataxStarRocksPojo;

import java.util.Map;

/**
 * starrocks writer构建类
 *
 * @author gyuan
 * @ClassName StarRocksWriter
 * @Version 1.0
 * @since 2023/02/26 23:08
 */
public class StarRocksWriter extends BaseWriterPlugin implements DataxWriterInterface {
    @Override
    public String getName() {
        return "starrockswriter";
    }

    @Override
    public Map<String, Object> buildStarRocks(DataxStarRocksPojo plugin) {
        Map<String, Object> writerObj = Maps.newLinkedHashMap();
        writerObj.put("name", getName());

        Map<String, Object> parameterObj = Maps.newLinkedHashMap();
        JobDatasource jobDatasource = plugin.getJobDatasource();
        parameterObj.put("username", jobDatasource.getJdbcUsername());
        String password = jobDatasource.getJdbcPassword();
        parameterObj.put("password", password == null ? "" : password);
        parameterObj.put("column", plugin.getRdbmsColumns());
        parameterObj.put("preSql", splitSql(plugin.getPreSql()));
        parameterObj.put("postSql", splitSql(plugin.getPostSql()));
        parameterObj.put("loadUrl", plugin.getLoadUrl().split(Constants.SPLIT_COMMA));

        Map<String, Object> connectionObj = Maps.newLinkedHashMap();
        connectionObj.put("table", plugin.getTables());
        connectionObj.put("jdbcUrl", jobDatasource.getJdbcUrl());
        connectionObj.put("selectedDatabase", plugin.getSelectedDatabase());

        parameterObj.put("connection", ImmutableList.of(connectionObj));
        writerObj.put("parameter", parameterObj);

        return writerObj;
    }


    @Override
    public Map<String, Object> sample() {
        return null;
    }
}
