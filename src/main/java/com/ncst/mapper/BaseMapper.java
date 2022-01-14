package com.ncst.mapper;

import com.alibaba.fastjson.JSONArray;
import com.ncst.dto.ExecuteResults;
import com.ncst.dto.QueryResults;
import com.ncst.rqlite.NodeUnavailableException;
import com.ncst.rqlite.Rqlite;
import com.ncst.rqlite.RqliteFactory;

import java.util.List;

/**
 * @author Lisy
 */
public interface BaseMapper<T> {
    Rqlite RQLITE = RqliteFactory.connect("http", "192.168.46.25", 4101);

    /**
     * @param queryParam DQL SQL
     * @return 查询数据json串
     */
    static String queryParam(String queryParam) {
        QueryResults query = null;
        try {
            query = RQLITE.Query(queryParam, Rqlite.ReadConsistencyLevel.WEAK);
        } catch (NodeUnavailableException e) {
            e.printStackTrace();
        }

        if (query == null) {
            return "";
        }else {
            float time = query.results[0].time;
            System.out.println("=======================select use time = " + time + "=====================");
            return query.results[0].toString();
        }
    }

    /**
     * DML 查询语句
     * @param sql DML SQL
     * @return 影响的行数
     */
    static Integer execute(String sql) {
        try {
            ExecuteResults execute = RQLITE.Execute(sql);
            return execute.results[0].rowsAffected;
        } catch (NodeUnavailableException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 批量发送 DML SQL
     * @param queryArray DML SQL Array
     * @param transaction 是否开启事务
     * @return 影响的行数
     */
    static Integer batchExecute(String[] queryArray, boolean transaction) {
        try {
            ExecuteResults execute = RQLITE.Execute(queryArray, transaction);
            return execute.results[0].rowsAffected;
        } catch (NodeUnavailableException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取数据库对象转换为对应的实体类
     * @param value json串
     * @return 对应的实体类
     */
    T getObj(JSONArray value);

    /**
     * 对应sql查询单个实体类
     * @param queryParam sql
     * @return 实体类
     */
    T getResultObj(String queryParam);

    /**
     * 对应sql查询实体类集合
     * @param queryParam sql
     * @return 实体类集合
     */
    List<T> getResultObjLists(String queryParam);
}
