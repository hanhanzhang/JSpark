package com.sdu.spark.utils;

import com.google.common.collect.Lists;
import org.apache.commons.collections.EnumerationUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Vector;

/**
 * @author hanhan.zhang
 * */
public class ChildFirstURLClassLoader extends MutableURLClassLoader {

    private ParentClassLoader parentClassLoader;

    public ChildFirstURLClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, null);
        this.parentClassLoader = new ParentClassLoader(parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
            return super.loadClass(name, resolve);
        } catch (ClassNotFoundException e){
            return parentClassLoader.loadClass(name, resolve);
        }
    }

    @Override
    public URL getResource(String name) {
        URL url = super.findResource(name);
        return url != null ? url : parentClassLoader.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        Enumeration<URL> childUrls = super.findResources(name);
        Enumeration<URL> parentUrls = parentClassLoader.getResources(name);
        Vector<URL> vector = new Vector<>();
        while (childUrls.hasMoreElements()) {
            vector.add(childUrls.nextElement());
        }
        while (parentUrls.hasMoreElements()) {
            vector.add(parentUrls.nextElement());
        }
        return vector.elements();
    }

    @Override
    public void addURL(URL url) {
        super.addURL(url);
    }
}
