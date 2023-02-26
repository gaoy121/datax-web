package com.wugui.datax.admin.tool.meta;

/**
 * StarRocks数据库 meta信息查询
 *
 * @author gyuan
 * @ClassName StarRocksDatabaseMeta
 * @Version 1.0
 * @since 2023/02/26 15:48
 */
public class StarRocksDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

    private volatile static StarRocksDatabaseMeta single;

    public static StarRocksDatabaseMeta getInstance() {
        if (single == null) {
            synchronized (StarRocksDatabaseMeta.class) {
                if (single == null) {
                    single = new StarRocksDatabaseMeta();
                }
            }
        }
        return single;
    }

    @Override
    public String getSQLQueryComment(String schemaName, String tableName, String columnName) {
        return String.format("SELECT COLUMN_COMMENT FROM information_schema.COLUMNS where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' and COLUMN_NAME = '%s'", schemaName, tableName, columnName);
    }

    @Override
    public String getSQLQueryPrimaryKey() {
        return "select column_name from information_schema.columns where table_schema=? and table_name=? and column_key = 'PRI'";
    }

    @Override
    public String getSQLQueryTables() {
        return "show tables";
    }

    @Override
    public String getSQLQueryColumns(String... args) {
        return "select column_name from information_schema.columns where table_schema=? and table_name=?";
    }
}
