package com.sdu.spark.scheduler;

import com.sdu.spark.storage.RDDInfo;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class StageInfo {

    public int stageId;
    public int attemptId;
    public String name;
    public int numTasks;
    public List<RDDInfo> rddInfos;
    public List<Integer> parentIds;
    public String details;

}
