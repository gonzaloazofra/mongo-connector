package com.despegar.integration.mongo.id;

import java.util.Date;


public class LongIdGenerator
    implements IdGenerator<Long> {

    @Override
    public Long generateId(String collectionName) {
        return (new Date()).getTime();
    }

    @Override
    public Boolean validateId(Object id) {
        return id != null;
    }

}
