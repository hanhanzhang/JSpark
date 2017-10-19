package com.sdu.spark;

import com.sdu.spark.utils.colleciton.ExternalAppendOnlyMap;
import com.sdu.spark.utils.scala.Product2;
import com.sdu.spark.utils.scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

/**
 * 数据聚合
 *
 * TODO: Task Metric
 *
 * @author hanhan.zhang
 * */
public class Aggregator<K, V, C> implements Serializable {

    public CreateAggregatorInitialValue<V, C> initial;
    public CreateAggregatorMergeValue<V, C> merge;
    public CreateAggregatorOutput<C> output;

    public Aggregator(CreateAggregatorInitialValue<V, C> initial,
                      CreateAggregatorMergeValue<V, C> merge,
                      CreateAggregatorOutput<C> output) {
        this.initial = initial;
        this.merge = merge;
        this.output = output;
    }

    /**
     * 按照Key聚合Value, 生成集合
     * */
    public Iterator<Tuple2<K, C>> combineValueByKey(Iterator<? extends Product2<K, V>> iter,
                                                    TaskContext context) {
        ExternalAppendOnlyMap<K, V, C> combiners = new ExternalAppendOnlyMap<>(initial, merge, output);
        combiners.insertAll(iter);
        updateMetrics(context, combiners);
        return combiners.iterator();
    }


    /**
     * 按照Key聚合Value集合, 生成输出结果
     * */
    public Iterator<Tuple2<K, C>> combineCombinersByKey(Iterator<? extends Product2<K, C>> iter,
                                                        TaskContext context) {
        // Scala.identity即原样输出输入内容
        CreateAggregatorInitialValue<C, C> identity = (val) -> val;
        CreateAggregatorMergeValue<C, C> merge = this.output::mergeCombiners;

        ExternalAppendOnlyMap<K, C, C> combiners = new ExternalAppendOnlyMap<>(identity, merge, output);
        combiners.insertAll(iter);
        updateMetrics(context, combiners);
        return combiners.iterator();
    }

    private void updateMetrics(TaskContext context, ExternalAppendOnlyMap<?, ?, ?> combiners) {
        // TODO: 待实现
    }
    
    /**
     * create the initial value of the aggregation.
     * */
    public interface CreateAggregatorInitialValue<V, C> {
        C createCombiner(V val);
    }

    /**
     * merge a new value into the aggregation result.
     * */
    public interface CreateAggregatorMergeValue<V, C> {
        C mergeValue(V val, C collection);
    }

    /**
     * merge outputs from multiple mergeValue function.
     * */
    public interface CreateAggregatorOutput<C> {
        C mergeCombiners(C collection1, C collection2);
    }

}
