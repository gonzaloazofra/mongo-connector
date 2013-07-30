package com.despegar.integration.mongo.connector;

import java.io.Serializable;

import org.springframework.util.Assert;

/**
 * 
 * represents the configuration of a page, in terms of a records offset, and a amount of records to take in an account 
 * @author jmontanaro
 */
public class Page
    implements Serializable {

    private static final long serialVersionUID = 1L;

    private int offset;
    private int limit;

    public Page(Integer offset, Integer limit) {
        Assert.notNull(offset, "Offset can't be null!");
        Assert.isTrue(offset >= 0, "Offset must be equals or greater than 0");
        Assert.notNull(limit, "Page number can't be null!");
        Assert.isTrue(limit > 0 || limit == -1, "Page number must be greater than 0 (or -1, means all the records)");

        this.offset = offset;
        this.limit = limit;
    }

    public int getOffset() {
        return this.offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLimit() {
        return this.limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

}
