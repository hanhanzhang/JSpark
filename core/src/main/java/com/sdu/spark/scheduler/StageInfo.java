package com.sdu.spark.scheduler;

import com.sdu.spark.rdd.RDD;
import com.sdu.spark.storage.RDDInfo;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
    public List<TaskLocation> taskLocalityPreferences;

    public StageInfo(int stageId, int attemptId, String name, int numTasks, List<RDDInfo> rddInfos,
                     List<Integer> parentIds, String details) {
        this(stageId, attemptId, name, numTasks, rddInfos, parentIds, details, Collections.emptyList());
    }

    public StageInfo(int stageId, int attemptId, String name, int numTasks,
                     List<RDDInfo> rddInfos, List<Integer> parentIds,
                     String details, List<TaskLocation> taskLocalityPreferences) {
        this.stageId = stageId;
        this.attemptId = attemptId;
        this.name = name;
        this.numTasks = numTasks;
        this.rddInfos = rddInfos;
        this.parentIds = parentIds;
        this.details = details;
        this.taskLocalityPreferences = taskLocalityPreferences;
    }

    public static StageInfo fromStage(Stage stage, int attemptId, int numTasks,
                                      List<TaskLocation> taskLocalityPreferences){
        List<RDDInfo> ancestorRddInfos = stage.rdd.getNarrowAncestors().stream()
                                                                        .map(RDDInfo::fromRDD)
                                                                        .collect(Collectors.toList());
        ancestorRddInfos.add(RDDInfo.fromRDD(stage.rdd));

        List<Integer> parentIds = stage.parents.stream().map(s -> s.id).collect(Collectors.toList());
        return new StageInfo(stage.id, attemptId, stage.name, numTasks, ancestorRddInfos, parentIds, stage.details, taskLocalityPreferences);
    }

    public static StageInfo fromStage(Stage stage, int attemptId) {
        return fromStage(stage, attemptId, stage.numTasks, Collections.emptyList());
    }
}
