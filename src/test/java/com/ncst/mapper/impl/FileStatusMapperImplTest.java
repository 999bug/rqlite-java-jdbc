package com.ncst.mapper.impl;

import com.ncst.dto.FileStatus;
import com.ncst.mapper.FileStatusMapper;
import com.ncst.mapper.impl.FileStatusMapperImpl;
import com.ncst.util.SnowflakeIdWorker;
import org.junit.Test;

import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FileStatusMapperImplTest {
    private static final FileStatusMapper mapper = new FileStatusMapperImpl();
    private static final SnowflakeIdWorker ID_WORKER = new SnowflakeIdWorker(0L, 0L);


    @Test
    public void testSelectFileStatusListByUuidAndBakDate() {
        List<FileStatus> fileStatuses =
                mapper.selectFileStatusListByUuidAndBakDate("RR33164F-AEBD-BD29-1296-EA5EA642B2DB", "2022-01-07_15-44-36");
        fileStatuses.forEach(System.out::println);
    }

    @Test
    public void testSelectCountForBakdate() {
        long l = System.currentTimeMillis();
        String str = mapper.selectCountForBakdate("2022-01-07_15-33-36");
        System.out.println(str);
        long l1 = System.currentTimeMillis();
        long useTime = l1 - l;
        System.out.println("useTime = " + useTime);
        double time = (double) useTime / 1000;
        String s = new Formatter().format("%.2f", time).toString();
        System.out.println("消耗时间 " + s + "秒");
    }

    @Test
    public void testGetStatusByUuidAndDateAndRelativePath() {
        FileStatus fileStatusBySequenceId =
                mapper.getStatusByUuidAndDateAndRelativePath("RR33164F-AEBD-BD29-1296-EA5EA642B2DB", "2022-01-07_15-44-36", "/df/b/c");
        System.out.println(fileStatusBySequenceId);
    }


    @Test
    public void testInsertFileStatus() {
        String relPath = "/df/b/c";
        String bakDate = "2022-01-07_15-44-36";
        FileStatus fileStatus = new FileStatus();
        fileStatus.setRluuid("RR33164F-AEBD-BD29-1296-EA5EA642B2DB");
        fileStatus.setSourcePath("hdfs://192.168.46.2:9000" + relPath);
        fileStatus.setRelPath(relPath);
        fileStatus.setTrunk(true);
        fileStatus.setTrunkSize(1073741824);
        fileStatus.setLastUpdateTime(1638760235635L);
        fileStatus.setHost("192.168.46.22");
        fileStatus.setSeqId(ID_WORKER.nextId());
        fileStatus.setOffset(0L);
        fileStatus.setDraft(false);
        fileStatus.setChecksum("dddddsdfsdf3dsfd");
        fileStatus.setBakDate(bakDate);
        fileStatus.setDeleted(true);
        fileStatus.setBakPath("E:\\rec\\A033164F-AEBD-BD29-1296-EA5EA642B2DB\\" + bakDate);
        int i = mapper.insertFileStatus(fileStatus);
        System.out.println("i = " + i);
    }

    @Test
    public void testSelectRecFileStatusList() {
        long l = System.currentTimeMillis();
        List<String> fileStatuses =
                mapper.selectRecRelPath("RR33164F-AEBD-BD29-1296-EA5EA642B2DB", "2022-01-07_15-19-36", "/d");

        Set<String> set = new HashSet<>();
        for (String str : fileStatuses) {
            int i = str.lastIndexOf("/");
            set.add(str.substring(0, i));
        }
        set.forEach(System.out::println);
        System.out.println(System.currentTimeMillis() - l);
    }
    static int insertNum = 0;

    @Test
    public void batchInsert() {
        long l = System.currentTimeMillis();
        int total = 10;
        for (int i = 0; i < total; i++) {
            insertData("/d/b9/c3", "2022-01-07_15-11-36");
            insertData("/d/b2/c", "2022-01-07_15-12-36");
            insertData("/d/b2/c", "2022-01-07_15-13-36");
            insertData("/d/b2/c", "2022-01-07_15-14-36");
            insertData("/d/b1/c", "2022-01-07_15-15-36");
            insertData("/d/b3/c", "2022-01-07_15-16-36");
            insertData("/d/b32/c3", "2022-01-07_15-17-36");
            insertData("/d/b6/c3", "2022-01-07_15-18-36");
            insertData("/d/b8/c3", "2022-01-07_15-19-36");


            // 相同数据不同时间
            insertData("/d/b9/c3", "2022-01-07_15-21-36");
            insertData("/d/b2/c", "2022-01-07_15-22-36");
            insertData("/d/b2/c", "2022-01-07_15-23-36");
            insertData("/d/b2/c", "2022-01-07_15-24-36");
            insertData("/d/b1/c", "2022-01-07_15-25-36");
            insertData("/d/b3/c", "2022-01-07_15-26-36");
            insertData("/d/b32/c3", "2022-01-07_15-27-36");
            insertData("/d/b6/c3", "2022-01-07_15-28-36");
            insertData("/d/b8/c3", "2022-01-07_15-29-36");

            // 相同时间不同数据
//            insertData("/d/b1/c3" + i, "2022-01-07_15-11-36");
//            insertData("/d/yu8/c" + i, "2022-01-07_15-12-36");
//            insertData("/d/b2/c8" + i, "2022-01-07_15-13-36");
//            insertData("/d/b43/c" + i, "2022-01-07_15-14-36");
//            insertData("/d/by/c" + i, "2022-01-07_15-15-36");
//            insertData("/d/bc/c" + i, "2022-01-07_15-16-36");
//            insertData("/d/c2/c3" + i, "2022-01-07_15-17-36");
//            insertData("/d/y6/c3" + i, "2022-01-07_15-18-36");
//            insertData("/d/u8/c3" + i, "2022-01-07_15-19-36");

            // 相同时间不同路径
//            insertData("/c/b1/c3" + i, "2022-01-07_15-11-36");
//            insertData("/c/yu8/c" + i, "2022-01-07_15-12-36");
//            insertData("/c/b2/c8" + i, "2022-01-07_15-13-36");
//            insertData("/c/b43/c" + i, "2022-01-07_15-14-36");
//            insertData("/c/by/c" + i, "2022-01-07_15-15-36");
//            insertData("/c/bc/c" + i, "2022-01-07_15-16-36");
//            insertData("/c/c2/c3" + i, "2022-01-07_15-17-36");
//            insertData("/c/y6/c3" + i, "2022-01-07_15-18-36");
//            insertData("/c/u8/c3" + i, "2022-01-07_15-19-36");
//
//            //相同路径不同时间
//            insertData("/c/b1/c3" + i, "2022-01-07_15-31-36");
//            insertData("/c/yu8/c" + i, "2022-01-07_15-32-36");
//            insertData("/c/b2/c8" + i, "2022-01-07_15-33-36");
//            insertData("/c/b43/c" + i, "2022-01-07_15-34-36");
//            insertData("/c/by/c" + i, "2022-01-07_15-35-36");
//            insertData("/c/bc/c" + i, "2022-01-07_15-36-36");
//            insertData("/c/c2/c3" + i, "2022-01-07_15-37-36");
//            insertData("/c/y6/c3" + i, "2022-01-07_15-38-36");
//            insertData("/c/u8/c3" + i, "2022-01-07_15-39-36");


        }
        long l1 = System.currentTimeMillis();
        long useTime = l1 - l;
        System.out.println("useTime = " + useTime);
        double time = (double) useTime / 1000;
        String s = new Formatter().format("%.2f", time).toString();
        System.out.println("消耗时间 " + s + "秒");
    }

    private void insertData(String relPath, String bakDate) {
        int commit = 30000;
        String[] strings = new String[commit];
        for (int i = 0; i < commit; i++) {

            FileStatus fileStatus = new FileStatus();
            fileStatus.setRluuid("RR33164F-AEBD-BD29-1296-EA5EA642B2DB");
            fileStatus.setSeqId(ID_WORKER.nextId());
            fileStatus.setSourcePath("hdfs://192.168.46.2:9000" + relPath);
            fileStatus.setRelPath(relPath + i);
            fileStatus.setTrunk(true);
            fileStatus.setTrunkSize(1073741824);
            fileStatus.setLastUpdateTime(1638760235635L);
            fileStatus.setHost("192.168.46.22");
            fileStatus.setOffset(0L);
            fileStatus.setDraft(false);
            fileStatus.setBakDate(bakDate);
            fileStatus.setBakPath("E:\\rec\\A033164F-AEBD-BD29-1296-EA5EA642B2DB\\" + bakDate + relPath);
            String format =
                    String.format("INSERT INTO I2FileStatus (rluuid, SourceFilePath, RelativePath, trunk, TrunkSize, LastUpdateTime," +
                                    " host, SequenceId, offset, draft, checksum, bakDate, deleted, bakPath) " +
                                    "VALUES ('%s', '%s', '%s', '%b', %d, %d, '%s', %d, %d, %b, '%s', '%s', %b, '%s' )",
                            fileStatus.getRluuid(), fileStatus.getSourcePath(), fileStatus.getRelPath(), fileStatus.isTrunk(),
                            fileStatus.getTrunkSize(), fileStatus.getLastUpdateTime(), fileStatus.getHost(), fileStatus.getSeqId(),
                            fileStatus.getOffset(), fileStatus.isDraft(), fileStatus.getChecksum(), fileStatus.getBakDate(),
                            fileStatus.isDeleted(), fileStatus.getBakPath());
            insertNum++;
            strings[i] = format;
        }

        int w = mapper.batchInsert(strings);
        if (w == 1) {
            System.out.println("已插入数据 " + insertNum);
        }else {
            System.out.println("error!!!!");
        }

    }

    @Test
    public void testUpdateFileStatus() {
        String relPath = "/df/b/c";
        String bakDate = "2022-01-07_15-44-36";
        FileStatus fileStatus = new FileStatus();
        fileStatus.setRluuid("RR33164F-AEBD-BD29-1296-EA5EA642B2DB");
        fileStatus.setSourcePath("hdfs://192.168.46.2:9000" + relPath);
        fileStatus.setRelPath(relPath);
        fileStatus.setTrunk(true);
        fileStatus.setTrunkSize(1073741824);
        fileStatus.setLastUpdateTime(1638760235635L);
        fileStatus.setHost("192.168.46.22");
        fileStatus.setSeqId(930110412202967040L);
        fileStatus.setOffset(0L);
        fileStatus.setDraft(false);
        fileStatus.setChecksum("RRRRRR");
        fileStatus.setBakDate(bakDate);
        fileStatus.setDeleted(true);
        fileStatus.setBakPath("E:\\rec\\A033164F-AEBD-BD29-1296-EA5EA642B2DB\\" + bakDate);
        int i = mapper.updateFileStatus(fileStatus);
        System.out.println("i = " + i);
    }


}
