package com.sdu.spark.utils;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public class CallSite implements Serializable {
    public static final String SHORT_FORM = "callSite.short";
    public static final String LONG_FORM = "callSite.long";
    public static final CallSite empty = new CallSite("", "");

    public String shortForm;
    public String longForm;

    public CallSite(String shortForm, String longForm) {
        this.shortForm = shortForm;
        this.longForm = longForm;
    }
}
