package com.sdu.spark;

import com.sdu.spark.rpc.RpcCallContext;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public class GetMapOutputMessage implements Serializable {

    public int shuffleId;

    public RpcCallContext context;

    public GetMapOutputMessage(int shuffleId, RpcCallContext context) {
        this.shuffleId = shuffleId;
        this.context = context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GetMapOutputMessage that = (GetMapOutputMessage) o;

        return shuffleId == that.shuffleId;

    }

    @Override
    public int hashCode() {
        return shuffleId;
    }
}
