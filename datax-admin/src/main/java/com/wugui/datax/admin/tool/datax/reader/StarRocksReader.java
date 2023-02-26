package com.wugui.datax.admin.tool.datax.reader;


import cn.hutool.core.util.StrUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.wugui.datax.admin.entity.JobDatasource;
import com.wugui.datax.admin.tool.pojo.DataxStarRocksPojo;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * starrocks reader 构建类
 *
 * @author gyuan
 * @ClassName StarRocksReader
 * @Version 1.0
 * @since 2023/02/26 23:07
 */
public class StarRocksReader extends BaseReaderPlugin implements DataxReaderInterface {
    @Override
    public String getName() {
        return "starrocksreader";
    }

    @Override
    public Map<String, Object> buildStarRocks(DataxStarRocksPojo plugin) {
        //构建
        Map<String, Object> readerObj = Maps.newLinkedHashMap();
        readerObj.put("name", getName());
        Map<String, Object> parameterObj = Maps.newLinkedHashMap();
        Map<String, Object> connectionObj = Maps.newLinkedHashMap();

        JobDatasource jobDatasource = plugin.getJobDatasource();
        parameterObj.put("username", jobDatasource.getJdbcUsername());
        String password = jobDatasource.getJdbcPassword();
        parameterObj.put("password", password == null ? "" : password);

        //判断是否是 querySql
        if (StrUtil.isNotBlank(plugin.getQuerySql())) {
            connectionObj.put("querySql", ImmutableList.of(plugin.getQuerySql()));
        } else {
            parameterObj.put("column", plugin.getRdbmsColumns());
            //判断是否有where
            if (StringUtils.isNotBlank(plugin.getWhereParam())) {
                parameterObj.put("where", plugin.getWhereParam());
            }
            connectionObj.put("table", plugin.getTables());
        }
        parameterObj.put("splitPk",plugin.getSplitPk());
        connectionObj.put("jdbcUrl", ImmutableList.of(jobDatasource.getJdbcUrl()));

        parameterObj.put("connection", ImmutableList.of(connectionObj));

        readerObj.put("parameter", parameterObj);

        return readerObj;
    }


    @Override
    public Map<String, Object> sample() {
        return null;
    }
}
