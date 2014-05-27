package com.despegar.integration.mongo.connector.impl;

import java.util.List;

import org.apache.commons.lang.mutable.MutableInt;
import org.springframework.beans.factory.InitializingBean;

import com.despegar.integration.domain.api.GenericIdentificableEntity;
import com.despegar.integration.mongo.connector.Handler;
import com.despegar.integration.mongo.connector.HandlerQuery;
import com.despegar.integration.mongo.connector.Page;
import com.despegar.integration.mongo.support.MongoDao;
import com.despegar.integration.mongo.support.MongoDaoFactory;
import com.mongodb.ReadPreference;

@SuppressWarnings("rawtypes")
public class MongoHandler<T extends GenericIdentificableEntity>
    implements Handler<T>, InitializingBean {

    private MongoDaoFactory mongoDaoFactory;
    protected Class<T> clazz;
    private String collectionName;

    protected MongoDao<T> mongoDao;

    @SuppressWarnings("unchecked")
    @Override
    public void afterPropertiesSet() throws Exception {
        this.mongoDao = this.mongoDaoFactory.getInstance(this.collectionName, this.clazz);
    }

    public <X extends Object> T get(final X id) {
        return this.mongoDao.findOne(id);
    }

    public T getOne() {
        HandlerQuery query = new HandlerQuery();
        query.setPage(new Page(1, 1));

        return this.getOne(query);
    }

    public T getOne(final HandlerQuery query) {
        final MongoHandlerQuery mhq = new MongoHandlerQuery(query);

        return this.mongoDao.findOne(mhq.getQuery(), mhq.getSortInfo(), mhq.getPage());
    }


    public List<T> getAll() {
        return this.getAll(null);
    }

    public List<T> getAll(final HandlerQuery query) {
        return this.getAll(query, null);
    }

    public List<T> getAll(final HandlerQuery query, final MutableInt count) {

        if (query == null) {
            return this.mongoDao.find();
        }

        final MongoHandlerQuery mongoQuery = new MongoHandlerQuery(query);

        return this.mongoDao.find(mongoQuery.getQuery(), null, mongoQuery.getSortInfo(), mongoQuery.getPage(), count,
            this.isCrucialDataIntegration(query));
    }

    public List<T> getAll(HandlerQuery query, MutableInt count, Integer pagingOffset, Integer pagingLimit) {
        query.setPage(new Page(pagingOffset, pagingLimit));
        return this.getAll(query, count);
    }

    public Integer count(final HandlerQuery query) {
        if (query == null) {
            return this.mongoDao.getTotalObjectsInCollection(this.collectionName);
        }

        final MongoHandlerQuery mongoQuery = new MongoHandlerQuery(query);
        return this.mongoDao.getTotalObjectsInCollection(this.collectionName, mongoQuery.getQuery());
    }

    @SuppressWarnings("unchecked")
    public <X extends Object> X add(final T t) {
        t.setId(null);
        return this.mongoDao.insert(t);
    }

    @Override
    public <X extends Object> X insertIfNotPresent(final T t) {
        return this.mongoDao.insert(t);
    }

    public <X extends Object> X save(final T t) {
        return this.mongoDao.updateOrInsert(t);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <X extends Object> X update(final HandlerQuery query, final HandlerQuery updateQuery, boolean upsert) {
        final MongoHandlerQuery mongoQuery = new MongoHandlerQuery(query);
        final MongoHandlerQuery mongoUpdateQuery = new MongoHandlerQuery(updateQuery);
        Object[] res = (Object[]) this.mongoDao.update(mongoQuery.getQuery(), mongoUpdateQuery.getQuery(), upsert);
        if (res.length == 1) {
            return (X) res[0];
        }
        return null;
    }

    @Override
    public T getAndUpdate(final HandlerQuery query, boolean remove, final HandlerQuery updateQuery, boolean returnNew,
        boolean upsert) {
        MongoHandlerQuery mhq = new MongoHandlerQuery(query);
        MongoHandlerQuery mhqUpdate = new MongoHandlerQuery(updateQuery);

        return this.mongoDao.findAndModify(mhq.getQuery(), null, mhq.getSortInfo(), remove, mhqUpdate.getQuery(), returnNew,
            upsert);
    }

    public <X extends Object> void remove(final X id) {
        this.mongoDao.delete(this.collectionName, id);
    }

    @Override
    public void remove(HandlerQuery query) {
        final MongoHandlerQuery mongoQuery = new MongoHandlerQuery(query);
        this.mongoDao.delete(this.collectionName, mongoQuery.getQuery());
    }

    public void removeAll() {
        this.mongoDao.dropCollection(this.collectionName);
    }

    public void setCollectionName(final String collectionName) {
        this.collectionName = collectionName;
    }

    public void setClazz(final Class<T> clazz) {
        this.clazz = clazz;
    }

    public void setMongoDaoFactory(final MongoDaoFactory mongoDaoFactory) {
        this.mongoDaoFactory = mongoDaoFactory;
    }

    public List<?> distinct(String key) {
        return this.mongoDao.distinct(key);
    }

    public List<?> distinct(String key, HandlerQuery query) {
        MongoHandlerQuery q = new MongoHandlerQuery(query);
        return this.mongoDao.distinct(key, q.getQuery());
    }

    private ReadPreference isCrucialDataIntegration(HandlerQuery query) {
        if (query.isCrucialDataIntegration()) {
            return ReadPreference.PRIMARY;
        }

        return null;
    }

    @Override
    public boolean exists(HandlerQuery query) {
        MongoHandlerQuery q = new MongoHandlerQuery(query);
        return this.mongoDao.exists(q.getQuery());
    }
}
