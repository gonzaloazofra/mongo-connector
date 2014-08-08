package com.despegar.integration.mongo.id;

import org.springframework.beans.factory.InitializingBean;

import com.despegar.integration.mongo.support.MongoDBConnection;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class LongIdGenerator
    implements IdGenerator<Long>, InitializingBean {

    private MongoDBConnection mongoDBConnection;
    private String counterCollectionName;
    private DBCollection collection;

    @Override
    public void afterPropertiesSet() throws Exception {
        DB db = this.mongoDBConnection.getDb();
        this.collection = db.getCollection(this.counterCollectionName);
    }

    @Override
    public Long generateId(String collectionName) {
        DBObject query = new BasicDBObject();
        query.put("_id", collectionName);

        DBObject update = new BasicDBObject();
        update.put("$inc", new BasicDBObject("sec", 1));

        DBObject response = this.collection.findAndModify(query, new BasicDBObject(), new BasicDBObject(), Boolean.FALSE,
            update, Boolean.TRUE, Boolean.TRUE);

        return (Long) response.get("sec");
    }

    @Override
    public Boolean validateId(Object id) {
        return id != null && (Long) id > 0;
    }

    @Override
    public void updateId(String collectionName, Object id) {
        DBObject existsQuery = new BasicDBObject();
        existsQuery.put("_id", collectionName);

        if (this.collection.findOne(existsQuery) == null) {
            existsQuery.put("sec", id);
            this.collection.insert(existsQuery);
            return;
        }

        existsQuery.put("sec", new BasicDBObject("$lte", id));

        DBObject update = new BasicDBObject();
        update.put("sec", id);

        this.collection.update(existsQuery, update);
    }

    public void setCounterCollectionName(String counterCollectionName) {
        this.counterCollectionName = counterCollectionName;
    }

    public void setMongoDBConnection(MongoDBConnection mongoDBConnection) {
        this.mongoDBConnection = mongoDBConnection;
    }
}
