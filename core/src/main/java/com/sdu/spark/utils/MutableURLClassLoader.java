package com.sdu.spark.utils;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * 重写的目的在于暴露方法:
 *
 * 1: {@link #addURL(URL)}
 *
 * 2: {@link #getURLs()}
 *
 * @author hanhan.zhang
 * */
public class MutableURLClassLoader extends URLClassLoader {

    public MutableURLClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public void addURL(URL url) {
        super.addURL(url);
    }

    @Override
    public URL[] getURLs() {
        return super.getURLs();
    }
}
