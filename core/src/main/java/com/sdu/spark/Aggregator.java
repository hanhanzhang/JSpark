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

    public InitialCollection<V, C> initialCollection;
    public AppendValue<V, C> appendValue;
    public CollectionToOutput<C> output;

    public Aggregator(InitialCollection<V, C> initialCollection,
                      AppendValue<V, C> appendValue,
                      CollectionToOutput<C> output) {
        this.initialCollection = initialCollection;
        this.appendValue = appendValue;
        this.output = output;
    }

    /**
     * 聚合Key对应Value(单条记录)
     * */
    public Iterator<Tuple2<K, C>> combineValueByKey(Iterator<Product2<K, V>> iter,
                                                    TaskContext context) {
        ExternalAppendOnlyMap<K, V, C> combiners = new ExternalAppendOnlyMap<>(initialCollection, appendValue, output);
        combiners.insertAll(iter);
        updateMetrics(context, combiners);
        return combiners.iterator();
    }


    /**
     * 聚合Key对应集合(多条记录)
     * */
    public Iterator<Tuple2<K, C>> combineCombinersByKey(Iterator<? extends Product2<K, C>> iter,
                                                        TaskContext context) {
        // Scala.identity即原样输出输入内容
        InitialCollection<C, C> identity = (val) -> val;
        AppendValue<C, C> merge = this.output::mergeCombiners;

        ExternalAppendOnlyMap<K, C, C> combiners = new ExternalAppendOnlyMap<>(identity, merge, output);
        combiners.insertAll(iter);
        updateMetrics(context, combiners);
        return combiners.iterator();
    }

    private void updateMetrics(TaskContext context, ExternalAppendOnlyMap<?, ?, ?> combiners) {
        // TODO: 待实现
    }
    
    /**
     * create the initialCollection value of the aggregation.
     * */
    public interface InitialCollection<V, C> {
        C createCollection(V val);
    }

    /**
     * appendValue a new value into the aggregation result.
     * */
    public interface AppendValue<V, C> {
        C appendValue(V val, C collection);
    }

    /**
     * appendValue outputs from multiple appendValue function.
     * */
    public interface CollectionToOutput<C> {
        C mergeCombiners(C collection1, C collection2);
    }

}
