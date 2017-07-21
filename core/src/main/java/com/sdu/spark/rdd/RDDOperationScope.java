package com.sdu.spark.rdd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hanhan.zhang
 * */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(value = {"id", "name", "parent"})
public class RDDOperationScope {

    private static final ObjectMapper jsonMapper = new ObjectMapper().registerModule(new SimpleModule());
    private static final AtomicInteger scopeCounter = new AtomicInteger(0);

    public String name;
    public RDDOperationScope parent;
    public String id;

    public RDDOperationScope(String name) {
        this(name, null);
    }

    public RDDOperationScope(String name, RDDOperationScope parent) {
        this.name = name;
        this.parent = parent;
        this.id = String.valueOf(nextScopeId());
    }


    public static RDDOperationScope fromJson(String s) {
        try {
            return jsonMapper.readValue(s, RDDOperationScope.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int nextScopeId() {
        return scopeCounter.getAndIncrement();
    }

}
