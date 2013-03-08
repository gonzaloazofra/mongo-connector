package com.despegar.integration.mongo.connector;

import java.io.Serializable;

import org.springframework.util.Assert;

public class Page
    implements Serializable {

    private static final long serialVersionUID = 1L;

    private int pageSize;
    private int pageNumber;

    public Page(Integer pageSize, Integer pageNumber) {
        Assert.notNull(pageSize, "Page size can't be null!");
        Assert.isTrue(pageSize > 0, "Page size must be greater than 0");
        Assert.notNull(pageNumber, "Page number can't be null!");
        Assert.isTrue(pageNumber > 0, "Page number must be greater than 0");

        this.pageSize = pageSize;
        this.pageNumber = pageNumber;
    }

    public int size() {
        return this.pageSize;
    }

    public int number() {
        return this.pageNumber;
    }

    public static int totalPagesCount(int totalSize, int pageSize) {
        Assert.isTrue(pageSize > 0, "Page size must be greater than 0");
        Assert.isTrue(totalSize >= pageSize, "Total size must be equal or greater than page size");

        return (totalSize % pageSize == 0) ? totalSize / pageSize : (totalSize / pageSize) + 1;
    }

}
