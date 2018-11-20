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

    public CombinerCreator<V, C> combinerCreator;
    public CombinerAdd<V, C> combinerAdd;
    public CombinerMerge<C> combinerMerge;

    public Aggregator(CombinerCreator<V, C> combinerCreator, CombinerAdd<V, C> combinerAdd, CombinerMerge<C> combinerMerge) {
        this.combinerCreator = combinerCreator;
        this.combinerAdd = combinerAdd;
        this.combinerMerge = combinerMerge;
    }

    /**
     * 聚合Key对应Value(单条记录)
     * */
    public Iterator<Tuple2<K, C>> combineValueByKey(Iterator<Product2<K, V>> iterator, TaskContext context) {
        ExternalAppendOnlyMap<K, V, C> combiners = new ExternalAppendOnlyMap<>(combinerCreator, combinerAdd, combinerMerge);
        combiners.insertAll(iterator);
        updateMetrics(context, combiners);
        return combiners.iterator();
    }


    /**
     * 聚合Key对应集合(多条记录)
     * */
    public Iterator<Tuple2<K, C>> combineCombinersByKey(Iterator<? extends Product2<K, C>> iterator, TaskContext context) {
        // Scala.identity即原样输出输入内容
        CombinerCreator<C, C> identity = (val) -> val;
        CombinerAdd<C, C> merge = this.combinerMerge::mergeCombiners;

        ExternalAppendOnlyMap<K, C, C> combiners = new ExternalAppendOnlyMap<>(identity, merge, combinerMerge);
        combiners.insertAll(iterator);
        updateMetrics(context, combiners);
        return combiners.iterator();
    }

    private void updateMetrics(TaskContext context, ExternalAppendOnlyMap<?, ?, ?> combiners) {
        // TODO: 待实现
    }
    
    /**
     * create the combinerCreator value of the aggregation.
     * */
    public interface CombinerCreator<V, C> {

        C createCombiner(V val);

    }

    /**
     * combinerAdd a new value into the aggregation result.
     * */
    public interface CombinerAdd<V, C> {

        C mergeValue(V val, C collection);

    }

    /**
     * combinerAdd outputs from multiple combinerAdd function.
     * */
    public interface CombinerMerge<C> {

        C mergeCombiners(C collection1, C collection2);

    }

}
