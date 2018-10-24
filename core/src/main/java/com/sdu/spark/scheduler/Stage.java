package com.sdu.spark.scheduler;

import com.google.common.collect.Sets;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.utils.CallSite;

import java.util.List;
import java.util.Set;

/**
 *
 * @author hanhan.zhang
 * */
@SuppressWarnings("unused")
public abstract class Stage {

    /** Stage身份标识 */
    private int id;
    /** Stage包含的RDD */
    private RDD<?> rdd;
    /** Stage的Task数量 */
    private int numTasks;
    /** Stage的父Stage集合 */
    private List<Stage> parents;
    /** Stage提交的第一个Job的标识 */
    private int firstJobId;
    /** Stage包含的Job集合 */
    private Set<Integer> jobIds;
    /** Stage下一次尝试的身份标识 */
    private int nextAttemptId;
    /** Stage分区数量 */
    private int numPartitions;
    /**  */
    private String name;
    /**  */
    private String details;
    /** Stage最近一次尝试信息 */
    private StageInfo latestInfo;
    /**  */
    private Set<Integer> fetchFailedAttemptIds;

    public Stage(int id,
                 RDD<?> rdd,
                 int numTasks,
                 List<Stage> parents,
                 int firstJobId,
                 CallSite callSite) {
        this.id = id;
        this.rdd = rdd;
        this.numTasks = numTasks;
        this.parents = parents;
        this.firstJobId = firstJobId;

        this.numPartitions = this.rdd.partitions().length;
        this.nextAttemptId = 0;
        this.latestInfo = StageInfo.fromStage(this, nextAttemptId);

        this.name = callSite.shortForm;
        this.details = callSite.longForm;

        this.jobIds = Sets.newHashSet();
        this.fetchFailedAttemptIds = Sets.newHashSet();
    }

    public void clearFailures() {
        fetchFailedAttemptIds.clear();
    }

    public void makeNewStageAttempt(int numPartitionsToCompute) {
        makeNewStageAttempt(numPartitionsToCompute, new TaskLocation[0][0]);
    }

    public void makeNewStageAttempt(int numPartitionsToCompute,
                                    TaskLocation[][] taskLocalityPreferences) {
        // TODO: TaskMetric
        latestInfo = StageInfo.fromStage(this,
                                         nextAttemptId,
                                         numPartitionsToCompute,
                                         taskLocalityPreferences);
        nextAttemptId += 1;
    }

    public StageInfo latestInfo() {
        return latestInfo;
    }

    public void addJobId(int jobId) {
        jobIds.add(jobId);
    }

    public void removeJobId(int jobId) {
        jobIds.remove(jobId);
    }

    public boolean stageJobEmpty() {
        return jobIds.isEmpty();
    }

    public boolean containsJobId(int jobId) {
        return jobIds.contains(jobId);
    }

    public Set<Integer> getJobIds() {
        return jobIds;
    }

    public abstract List<Integer> findMissingPartitions();

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public RDD<?> getRdd() {
        return rdd;
    }

    public int getNumTasks() {
        return numTasks;
    }

    public List<Stage> getParents() {
        return parents;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public String getDetails() {
        return details;
    }

    public int getFirstJobId() {
        return firstJobId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Stage stage = (Stage) o;

        return id == stage.id;

    }

    @Override
    public int hashCode() {
        return id;
    }
}
