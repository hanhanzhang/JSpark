package com.sdu.spark.storage;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author hanhan.zhang
 * */
public class RandomBlockReplicationPolicy implements BlockReplicationPolicy {

    private static final Logger LOGGER = LoggerFactory.getLogger(RandomBlockReplicationPolicy.class);

    @Override
    public List<BlockManagerId> prioritize(BlockInfoManager blockInfoManager,
                                           Set<BlockManagerId> peers, BlockId blockId, int numReplicas) {
        Random random = new Random(blockId.hashCode());
        LOGGER.debug("Input peers : {}", StringUtils.join(peers, ", "));
        return getRandomSample(Lists.newArrayList(peers), numReplicas, random);
    }

    private static <T> List<T> getRandomSample(List<T> elems, int m, Random r) {
        if (elems.size() > m) {
            return getSampleIds(elems.size(), m, r).stream().map(elems::get).collect(Collectors.toList());
        } else {
            Collections.shuffle(elems);
            return elems;
        }
    }

    /**
     * @param n : total number of indices
     * @param m : number of samples needed
     * */
    private static List<Integer> getSampleIds(int n, int m, Random r) {
        Set<Integer> sampleIds = Sets.newHashSet();
        // 需要遍历m次
        for (int i = n - m + 1; i <= n; ++i) {
            int t = r.nextInt(i) + 1;
            if (sampleIds.contains(t)) {
                sampleIds.add(i);
            } else {
                sampleIds.add(t);
            }
        }
        return sampleIds.stream().map(index -> index -1).collect(Collectors.toList());
    }

//    public static void main(String[] args) {
//        List<Integer> list = getSampleIds(5, 3, new Random());
//        System.out.println(list);
//    }
}
