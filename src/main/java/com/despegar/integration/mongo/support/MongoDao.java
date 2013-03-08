package com.despegar.integration.mongo.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import net.vz.mongodb.jackson.DBCursor;
import net.vz.mongodb.jackson.JacksonDBCollection;
import net.vz.mongodb.jackson.WriteResult;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.bson.types.ObjectId;

import com.despegar.integration.domain.api.IdentificableEntity;
import com.despegar.integration.mongo.connector.Page;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class MongoDao<T extends IdentificableEntity> {

    private DB mongoDb;
    private Class<T> clazz;
    private JacksonDBCollection<T, Object> coll;

    public MongoDao(DB mongoDb, String collection, Class<T> clazz) {
        this.mongoDb = mongoDb;
        this.clazz = clazz;
        ObjectMapper mapper = new ObjectMapper();

        mapper.setPropertyNamingStrategy(new IdWithUnderscoreStrategy());
        mapper.setSerializationInclusion(Include.NON_NULL);

        this.coll = JacksonDBCollection.wrap(this.mongoDb.getCollection(collection), this.clazz, Object.class, mapper);
    }

    public MongoDao(DB mongoDb, String collection, ObjectMapper mapper, Class<T> clazz) {
        this.mongoDb = mongoDb;
        this.clazz = clazz;

        mapper.setPropertyNamingStrategy(new IdWithUnderscoreStrategy());
        mapper.setSerializationInclusion(Include.NON_NULL);

        this.coll = JacksonDBCollection.wrap(this.mongoDb.getCollection(collection), this.clazz, Object.class, mapper);
    }

    public T findOne() {
        return this.findOne(new BasicDBObject());
    }

    public T findOne(DBObject query) {
        return this.coll.findOne(query, new BasicDBObject());
    }

    public T findOne(String id) {
        return this.coll.findOne(new BasicDBObject("_id", id), new BasicDBObject());
    }

    public List<T> find() {
        return this.find(new BasicDBObject());
    }

    public List<T> find(DBObject query) {
        return this.find(query, new BasicDBObject());
    }

    public List<T> find(DBObject query, DBObject fields) {
        return this.findUsingSortInfo(query, fields, null);
    }

    public List<T> findUsingSortInfo(DBObject query, DBObject fields, DBObject sortInfo) {
        return this.find(query, fields, sortInfo, null);
    }

    public List<T> findUsingPage(DBObject query, DBObject fields, Page page) {
        return this.find(query, fields, null, page);
    }

    public List<T> find(DBObject query, DBObject fields, DBObject sortInfo, Page page) {
        return this.find(query, fields, sortInfo, page, null);
    }

    public List<T> find(DBObject query, DBObject fields, DBObject sortInfo, Page page, MutableInt count) {
        DBCursor<T> cursor = this.coll.find(query, fields);

        if (count != null) {
            count.setValue(cursor.count());
        }

        if (sortInfo != null) {
            cursor = cursor.sort(sortInfo);
        }

        if (page != null) {
            cursor = cursor.skip((page.number() - 1) * page.size());
            cursor = cursor.limit(page.size());
        }

        List<T> ret = new ArrayList<T>();
        while (cursor.hasNext()) {
            ret.add(cursor.next());
        }

        return ret;
    }

    public T findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update,
        boolean returnNew, boolean upsert) {
        return this.coll.findAndModify(query, fields, sort, remove, update, returnNew, upsert);
    }

    public String insert(T value) {
        return this.insert(value, WriteConcern.NORMAL);
    }

    public String insert(T value, WriteConcern concern) {
        if (StringUtils.isEmpty(value.getId())) {
            value.setId(new ObjectId().toString());
        }

        WriteResult<T, Object> insert = this.coll.insert(value, concern);

        return insert.getSavedObject().getId();
    }

    public Object update(DBObject query, DBObject value, boolean upsert, boolean multi, WriteConcern concern) {
        WriteResult<T, Object> update = this.coll.update(query, value, upsert, multi, concern);

        return update.getSavedIds();
    }

    public Object update(DBObject query, DBObject value, boolean upsert, WriteConcern concern) {
        return this.update(query, value, upsert, false, concern);
    }

    public Object update(DBObject query, DBObject value, WriteConcern concern) {
        return this.update(query, value, false, false, concern);
    }

    public String updateById(String id, T value) {
        this.coll.updateById(id, value);

        return id;
    }

    public String updateOrInsert(T value) {
        return this.updateOrInsert(value, WriteConcern.SAFE);
    }

    public String updateOrInsert(T value, WriteConcern concern) {
        String ret = null;
        if (StringUtils.isEmpty(value.getId()) || this.findOne(value.getId()) == null) {
            ret = this.insert(value, concern);
        } else {
            ret = this.updateById(value.getId(), value);
        }

        return ret;
    }

    public int getTotalObjectsInCollection(String collection) {
        return this.getTotalObjectsInCollection(collection, new BasicDBObject());
    }

    public int getTotalObjectsInCollection(String collection, DBObject key) {
        return this.coll.find(key).count();
    }

    public Set<String> getCollectionNames() {
        return this.mongoDb.getCollectionNames();
    }

    public void dropCollection(String collection) {
        DBCollection coll = this.mongoDb.getCollection(collection);
        coll.drop();
    }

    public void renameCollection(String collection, String newName) {
        this.renameCollection(collection, newName, false);
    }

    public void renameCollection(String collection, String newName, Boolean dropTarget) {
        DBCollection coll = this.mongoDb.getCollection(collection);
        coll.rename(newName, dropTarget);
    }

    public void delete(String collection, DBObject query) {
        DBCollection coll = this.mongoDb.getCollection(collection);
        coll.remove(query);
    }

    public void delete(String collection, String id) {
        this.delete(collection, new BasicDBObject("_id", id));
    }

    public void ensureIndex(String collection, DBObject index) {
        DBCollection coll = this.mongoDb.getCollection(collection);
        coll.ensureIndex(index);
    }
}
