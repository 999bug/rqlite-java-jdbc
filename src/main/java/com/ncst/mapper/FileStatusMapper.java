package com.ncst.mapper;

import com.ncst.dto.FileStatus;

import java.util.List;

/**
 * @author Lisy
 */
public interface FileStatusMapper extends BaseMapper<FileStatus> {

    /**
     * 插入数据
     */
    int insertFileStatus(FileStatus fileStatus);

    /**
     * 批量插入
     */
    int batchInsert(String[] strings);


    int updateFileStatus(FileStatus fStatus);


    List<FileStatus> selectFileStatusListByUuidAndBakDate(String rluuid, String bakDate);


    FileStatus getStatusByUuidAndDateAndRelativePath(String rluuid, String bakDate, String relativePath);


    int deleteFileStatusByUuidAndBakDate(String rluuid, String bakDate);


    /**
     * 获取恢复备份使用的文件状态集合
     *
     * @param rluuid       规则uuid
     * @param bakDate      备份时间，不可以为null yyyy-MM-dd_HH-mm-ss
     * @param relativePath 待还原目录相对路径，使用此参数模糊查询
     * @return 文件状态的集合
     */
    List<String> selectRecRelPath(String rluuid,
                                  String bakDate,
                                  String relativePath);

    String selectCountForBakdate(String param);
}
