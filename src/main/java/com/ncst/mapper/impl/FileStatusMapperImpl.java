package com.ncst.mapper.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ncst.dto.FileStatus;
import com.ncst.mapper.BaseMapper;
import com.ncst.mapper.FileStatusMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lisy
 */
public class FileStatusMapperImpl implements FileStatusMapper {


    @Override
    public int insertFileStatus(FileStatus fileStatus) {
        String format =
                String.format("INSERT INTO I2FileStatus (rluuid, SourceFilePath, RelativePath, trunk, TrunkSize, LastUpdateTime," +
                                " host, SequenceId, offset, draft, checksum, bakDate, deleted, bakPath) " +
                                "VALUES ('%s', '%s', '%s', '%b', %d, %d, '%s', %d, %d, %b, '%s', '%s', %b, '%s' )",
                        fileStatus.getRluuid(), fileStatus.getSourcePath(), fileStatus.getRelPath(), fileStatus.isTrunk(),
                        fileStatus.getTrunkSize(), fileStatus.getLastUpdateTime(), fileStatus.getHost(), fileStatus.getSeqId(),
                        fileStatus.getOffset(), fileStatus.isDraft(), fileStatus.getChecksum(), fileStatus.getBakDate(),
                        fileStatus.isDeleted(), fileStatus.getBakPath());

        return BaseMapper.execute(format);
    }

    @Override
    public int batchInsert(String[] strings) {
        return BaseMapper.batchExecute(strings, true);
    }


    @Override
    public int updateFileStatus(FileStatus fStatus) {
        String query =
                String.format("UPDATE I2FileStatus set TrunkSize = %d, LastUpdateTime = %d, checksum = '%s', draft = %b WHERE SequenceId = %d",
                        fStatus.getTrunkSize(), fStatus.getLastUpdateTime(), fStatus.getChecksum(), fStatus.isDraft(), fStatus.getSeqId());

        return BaseMapper.execute(query);
    }

    @Override
    public List<FileStatus> selectFileStatusListByUuidAndBakDate(String rluuid, String bakDate) {
        String query =
                String.format("SELECT * FROM (SELECT * FROM I2FileStatus  where rluuid = '%s' " +
                        "AND bakDate <= '%s' ORDER BY bakDate desc) GROUP BY RelativePath", rluuid, bakDate);
        return getResultObjLists(query);
    }



    @Override
    public FileStatus getStatusByUuidAndDateAndRelativePath(String rluuid, String bakDate, String relativePath) {
        String format =
                String.format("SELECT * FROM I2FileStatus WHERE rluuid = '%s' " +
                        "AND bakDate <= '%s' AND RelativePath = '%s' ORDER BY bakDate DESC LIMIT 0, 1", rluuid, bakDate, relativePath);
        return getResultObj(format);
    }

    @Override
    public int deleteFileStatusByUuidAndBakDate(String rluuid, String bakDate) {
        String format =
                String.format("DELETE FROM I2FileStatus WHERE rluuid = '%s' AND bakDate = '%s'", rluuid, bakDate);
        return BaseMapper.execute(format);
    }

    @Override
    public List<FileStatus> getResultObjLists(String queryParam) {
        String result = BaseMapper.queryParam(queryParam);
        JSONObject jsonObject = JSONObject.parseObject(result);

        JSONArray values = jsonObject.getJSONArray("values");
        if (values == null) {
            return null;
        } else {
            List<FileStatus> list = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) {
                JSONArray value = values.getJSONArray(i);
                list.add(getObj(value));
            }
            return list;
        }
    }


    @Override
    public FileStatus getObj(JSONArray value) {
        String rlUuid = value.getString(0);
        String sourceFilePath = value.getString(1);
        String relativePath = value.getString(2);
        boolean trunk = value.getBoolean(3);
        Integer trunkSize = value.getInteger(4);
        Long lastUpdateTime = value.getLong(5);
        String host = value.getString(6);
        Long sequenceId = value.getLong(7);
        Long offset = value.getLong(8);
        boolean draft = value.getBoolean(9);
        String checkSum = value.getString(10);
        String bakDate1 = value.getString(11);
        boolean deleted = value.getBoolean(12);
        String bakPath = value.getString(13);

        return new FileStatus(rlUuid, sourceFilePath, relativePath, trunk, trunkSize, lastUpdateTime,
                host, sequenceId, offset, draft, checkSum, bakDate1, deleted, bakPath);
    }

    @Override
    public FileStatus getResultObj(String queryParam) {
        String result = BaseMapper.queryParam(queryParam);
        JSONObject jsonObject = JSONObject.parseObject(result);
        JSONArray values = jsonObject.getJSONArray("values");
        if (values == null) {
            return null;
        } else {
            JSONArray value = values.getJSONArray(0);
            return getObj(value);
        }
    }

    @Override
    public List<String> selectRecRelPath(String rluuid, String bakDate, String relativePath) {
        String format =
                String.format("SELECT RelativePath  FROM (SELECT RelativePath FROM I2FileStatus  where rluuid = '%s' AND bakDate <= '%s' AND RelativePath LIKE '%s' || '%%' ORDER BY bakDate desc )GROUP BY RelativePath", rluuid, bakDate, relativePath);
        System.out.println("format = " + format);
        return getStringList(format);
    }

    @Override
    public String selectCountForBakdate(String param) {
        String format =
                String.format("SELECT  count(*) FROM I2FileStatus where bakDate = '%s' ", param);

        String result = BaseMapper.queryParam(format);
        JSONObject jsonObject = JSONObject.parseObject(result);
        JSONArray values = jsonObject.getJSONArray("values");
        JSONArray value = values.getJSONArray(0);
        return value.getString(0);
    }

    private List<String> getStringList(String format) {
        String result = BaseMapper.queryParam(format);
        JSONObject jsonObject = JSONObject.parseObject(result);
        JSONArray values = jsonObject.getJSONArray("values");

        if (values == null) {
            return null;
        } else {
            List<String> list = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) {
                JSONArray value = values.getJSONArray(i);
                list.add(value.getString(0));
            }
            return list;
        }
    }
}
