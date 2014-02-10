package com.despegar.integration.mongo.id;

import org.springframework.beans.factory.InitializingBean;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class LongIdGenerator
    implements IdGenerator<Long>, InitializingBean {

    private DB mongoDb;
    private String counterCollectionName;
    private DBCollection collection;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.collection = this.mongoDb.getCollection(this.counterCollectionName);
    }

    @Override
    public Long generateId(String collectionName) {
        DBObject query = new BasicDBObject();
        query.put("_id", collectionName);

        DBObject update = new BasicDBObject();
        update.put("$inc", "{sec:1}");

        DBObject response = this.collection.findAndModify(query, null, null, Boolean.FALSE, update, Boolean.TRUE,
            Boolean.TRUE);

        return (Long) response.get("sec");
    }

    @Override
    public Boolean validateId(Object id) {
        return id != null && (Long) id > 0;
    }

    @Override
    public void updateId(String collectionName, Object id) {
        DBObject query = new BasicDBObject();
        query.put("sec", new BasicDBObject("$gt", id));
        query.put("_id", collectionName);

        DBObject update = new BasicDBObject();
        update.put("$inc", new BasicDBObject("sec", 1));

        this.collection.update(query, update, Boolean.TRUE, Boolean.FALSE);
    }

    public void setCounterCollectionName(String counterCollectionName) {
        this.counterCollectionName = counterCollectionName;
    }

    public void setMongoDb(DB mongoDb) {
        this.mongoDb = mongoDb;
    }
}
