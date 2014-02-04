package com.despegar.integration.mongo.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import net.vz.mongodb.jackson.DBCursor;
import net.vz.mongodb.jackson.JacksonDBCollection;
import net.vz.mongodb.jackson.WriteResult;
import net.vz.mongodb.jackson.internal.MongoJacksonMapperModule;

import org.apache.commons.lang.mutable.MutableInt;

import com.despegar.integration.domain.api.GenericIdentificableEntity;
import com.despegar.integration.mongo.connector.Page;
import com.despegar.integration.mongo.id.IdGenerator;
import com.despegar.integration.mongo.id.StringIdGenerator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

@SuppressWarnings("rawtypes")
public class MongoDao<T extends GenericIdentificableEntity> {

    private DB mongoDb;
    private Class<T> clazz;
    private JacksonDBCollection<T, Object> coll;
    private IdGenerator idGenerator;

    @Deprecated
    public MongoDao(DB mongoDb, String collection, Class<T> clazz) {
        this(mongoDb, collection, new ObjectMapper(), clazz, new StringIdGenerator());
    }

    public MongoDao(DB mongoDb, String collection, ObjectMapper mapper, Class<T> clazz, IdGenerator idGenerator) {
        this.mongoDb = mongoDb;
        this.clazz = clazz;
        this.idGenerator = idGenerator;

        mapper.setPropertyNamingStrategy(new IdWithUnderscoreStrategy());
        mapper.setSerializationInclusion(Include.NON_NULL);
        MongoJacksonMapperModule.configure(mapper);

        this.coll = JacksonDBCollection.wrap(this.mongoDb.getCollection(collection), this.clazz, Object.class, mapper);
    }

    public T findOne() {
        return this.findOne(new BasicDBObject());
    }

    public T findOne(ReadPreference readPreference) {
        return this.findOne(new BasicDBObject(), readPreference);
    }

    public T findOne(DBObject query) {
        return this.findOne(query, new BasicDBObject(), new Page(0, 1));
    }

    public T findOne(DBObject query, ReadPreference readPreference) {
        return this.findOne(query, new BasicDBObject(), new Page(0, 1), readPreference);
    }

    public T findOne(DBObject query, DBObject sortInfo, Page page) {
        List<T> list = this.find(query, new BasicDBObject(), sortInfo, page);
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
    }

    public T findOne(DBObject query, DBObject sortInfo, Page page, ReadPreference readPreference) {
        List<T> list = this.find(query, new BasicDBObject(), sortInfo, page, readPreference);
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
    }

    public <X extends Object> T findOne(X id) {
        return this.coll.findOneById(id, new BasicDBObject());
    }

    public List<T> find() {
        return this.find(new BasicDBObject());
    }

    public List<T> find(ReadPreference readPreference) {
        return this.find(new BasicDBObject(), readPreference);
    }

    public List<T> find(DBObject query) {
        return this.find(query, new BasicDBObject());
    }

    public List<T> find(DBObject query, ReadPreference readPreference) {
        return this.find(query, new BasicDBObject(), readPreference);
    }

    public List<T> find(DBObject query, DBObject fields) {
        return this.findUsingSortInfo(query, fields, null);
    }

    public List<T> find(DBObject query, DBObject fields, ReadPreference readPreference) {
        return this.findUsingSortInfo(query, fields, null, readPreference);
    }

    public List<T> findUsingSortInfo(DBObject query, DBObject fields, DBObject sortInfo) {
        return this.find(query, fields, sortInfo, null);
    }

    public List<T> findUsingSortInfo(DBObject query, DBObject fields, DBObject sortInfo, ReadPreference readPreference) {
        return this.find(query, fields, sortInfo, null, readPreference);
    }

    public List<T> findUsingPage(DBObject query, DBObject fields, Page page) {
        return this.find(query, fields, null, page);
    }

    public List<T> findUsingPage(DBObject query, DBObject fields, Page page, ReadPreference readPreference) {
        return this.find(query, fields, null, page, readPreference);
    }

    public List<T> find(DBObject query, DBObject fields, DBObject sortInfo, Page page) {
        return this.find(query, fields, sortInfo, page, null, this.coll.getReadPreference());
    }

    public List<T> find(DBObject query, DBObject fields, DBObject sortInfo, Page page, ReadPreference readPreference) {
        return this.find(query, fields, sortInfo, page, null, readPreference);
    }

    public List<T> find(DBObject query, DBObject fields, DBObject sortInfo, Page page, MutableInt count) {
        return this.find(query, fields, sortInfo, page, count, this.coll.getReadPreference());
    }

    public List<T> find(DBObject query, DBObject fields, DBObject sortInfo, Page page, MutableInt count,
        ReadPreference readPreference) {
        DBCursor<T> cursor = this.coll.find(query, fields);

        if (count != null) {
            count.setValue(cursor.count());
        }

        if (sortInfo != null) {
            cursor = cursor.sort(sortInfo);
        }

        if (readPreference != null) {
            cursor.setReadPreference(readPreference);
        }

        if (page != null) {
            cursor = cursor.skip(page.getOffset());
            cursor = cursor.limit(page.getLimit());
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

    public List<?> distinct(String key) {
        return this.coll.distinct(key);
    }

    public <X extends Object> X insert(T value) {
        return this.insert(value, WriteConcern.NORMAL);
    }

    @SuppressWarnings("unchecked")
    public <X extends Object> X insert(T value, WriteConcern concern) {
        if (!this.idGenerator.validateId(value.getId())) {
            value.setId(this.idGenerator.generateId(this.coll.getName()));
        }

        WriteResult<T, Object> insert = this.coll.insert(value, concern);

        return (X) insert.getSavedObject().getId();
    }

    public Object update(DBObject query, DBObject value, boolean upsert, boolean multi, WriteConcern concern) {
        WriteResult<T, Object> update = this.coll.update(query, value, upsert, multi, concern);

        return update.getSavedIds();
    }

    public Object update(BasicDBObject query, BasicDBObject value, boolean upsert) {
        return this.update(query, value, upsert, WriteConcern.SAFE);
    }

    public Object update(DBObject query, DBObject value, boolean upsert, WriteConcern concern) {
        return this.update(query, value, upsert, false, concern);
    }

    public Object update(DBObject query, DBObject value, WriteConcern concern) {
        return this.update(query, value, false, false, concern);
    }

    public Object update(T query, T value, boolean upsert, boolean multi, WriteConcern concern) {
        WriteResult<T, Object> update = this.coll.update(query, value, upsert, multi, concern);

        return update.getSavedIds();
    }

    public Object update(T query, T value, boolean upsert) {
        return this.update(query, value, upsert, WriteConcern.SAFE);
    }

    public Object update(T query, T value, boolean upsert, WriteConcern concern) {
        return this.update(query, value, upsert, false, concern);
    }

    public Object update(T query, T value, WriteConcern concern) {
        return this.update(query, value, false, false, concern);
    }

    public <X extends Object> X updateById(X id, T value) {
        this.coll.updateById(id, value);

        return id;
    }

    public <X extends Object> X updateOrInsert(T value) {
        return this.updateOrInsert(value, WriteConcern.SAFE);
    }

    @SuppressWarnings("unchecked")
    public <X extends Object> X updateOrInsert(T value, WriteConcern concern) {
        X ret = null;
        if (!this.idGenerator.validateId(value.getId()) || this.findOne(value.getId()) == null) {
            ret = this.insert(value, concern);
        } else {
            ret = (X) this.updateById(value.getId(), value);
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

    public <X extends Object> void delete(String collection, X id) {
        this.delete(collection, new BasicDBObject("_id", id));
    }

    public void ensureIndex(String collection, DBObject index) {
        DBCollection coll = this.mongoDb.getCollection(collection);
        coll.ensureIndex(index);
    }


}
