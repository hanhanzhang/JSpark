package com.sdu.spark.scheduler;

import com.sdu.spark.storage.RDDInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TODO: TaskMetric
 *
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
    // 行表示分区, 列表示Task运行位置
    public TaskLocation[][] taskLocalityPreferences;

    /** When this stage was submitted from the DAGScheduler to a TaskScheduler. */
    private long submissionTime = -1;
    /** Time when all tasks in the stage completed or when the stage was cancelled. */
    private long completionTime = -1;
    /** If the stage failed, the reason why. */
    private String failureReason;

    public StageInfo(int stageId,
                     int attemptId,
                     String name,
                     int numTasks,
                     List<RDDInfo> rddInfos,
                     List<Integer> parentIds,
                     String details) {
        this(stageId, attemptId, name, numTasks, rddInfos, parentIds, details, new TaskLocation[0][0]);
    }

    public StageInfo(int stageId,
                     int attemptId,
                     String name,
                     int numTasks,
                     List<RDDInfo> rddInfos,
                     List<Integer> parentIds,
                     String details,
                     TaskLocation[][] taskLocalityPreferences) {
        this.stageId = stageId;
        this.attemptId = attemptId;
        this.name = name;
        this.numTasks = numTasks;
        this.rddInfos = rddInfos;
        this.parentIds = parentIds;
        this.details = details;
        this.taskLocalityPreferences = taskLocalityPreferences;
    }

    public long submissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(long submissionTime) {
        this.submissionTime = submissionTime;
    }

    public void setCompletionTime(long time) {
        completionTime = time;
    }

    public void stageFailed(String reason) {
        failureReason = reason;
        completionTime = System.currentTimeMillis();
    }

    public String getStatusString() {
        if (completionTime > 0) {
            if (StringUtils.isNotEmpty(failureReason)) {
                return "failed";
            } else {
                return "succeeded";
            }
        } else {
            return "running";
        }
    }

    public static StageInfo fromStage(Stage stage,
                                      int attemptId,
                                      int numTasks,
                                      TaskLocation[][] taskLocalityPreferences){
        List<RDDInfo> ancestorRddInfos = stage.rdd.getNarrowAncestors().stream()
                                                                        .map(RDDInfo::fromRDD)
                                                                        .collect(Collectors.toList());
        ancestorRddInfos.add(RDDInfo.fromRDD(stage.rdd));

        List<Integer> parentIds = stage.parents.stream().map(s -> s.id).collect(Collectors.toList());
        return new StageInfo(stage.id,
                             attemptId,
                             stage.name,
                             numTasks,
                             ancestorRddInfos,
                             parentIds,
                             stage.details,
                             taskLocalityPreferences);
    }

    public static StageInfo fromStage(Stage stage, int attemptId) {
        return fromStage(stage, attemptId, stage.numTasks, new TaskLocation[0][0]);
    }
}
