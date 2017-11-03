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

    public Initializer<V, C> initializer;
    public AppendValue<V, C> appendValue;
    public Combiner<C> combiner;

    public Aggregator(Initializer<V, C> initializer,
                      AppendValue<V, C> appendValue,
                      Combiner<C> combiner) {
        this.initializer = initializer;
        this.appendValue = appendValue;
        this.combiner = combiner;
    }

    /**
     * 聚合Key对应Value(单条记录)
     * */
    public Iterator<Tuple2<K, C>> combineValueByKey(Iterator<Product2<K, V>> iter,
                                                    TaskContext context) {
        ExternalAppendOnlyMap<K, V, C> combiners = new ExternalAppendOnlyMap<>(initializer, appendValue, combiner);
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
        Initializer<C, C> identity = (val) -> val;
        AppendValue<C, C> merge = this.combiner::mergeCombiners;

        ExternalAppendOnlyMap<K, C, C> combiners = new ExternalAppendOnlyMap<>(identity, merge, combiner);
        combiners.insertAll(iter);
        updateMetrics(context, combiners);
        return combiners.iterator();
    }

    private void updateMetrics(TaskContext context, ExternalAppendOnlyMap<?, ?, ?> combiners) {
        // TODO: 待实现
    }
    
    /**
     * create the initializer value of the aggregation.
     * */
    public interface Initializer<V, C> {
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
    public interface Combiner<C> {
        C mergeCombiners(C collection1, C collection2);
    }

}
