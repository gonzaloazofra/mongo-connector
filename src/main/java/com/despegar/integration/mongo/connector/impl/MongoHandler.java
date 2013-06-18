package com.despegar.integration.mongo.connector.impl;

import java.util.List;

import org.apache.commons.lang.mutable.MutableInt;
import org.springframework.beans.factory.InitializingBean;

import com.despegar.integration.domain.api.IdentificableEntity;
import com.despegar.integration.mongo.connector.Handler;
import com.despegar.integration.mongo.connector.HandlerQuery;
import com.despegar.integration.mongo.connector.Page;
import com.despegar.integration.mongo.support.MongoDao;
import com.despegar.integration.mongo.support.MongoDaoFactory;
import com.mongodb.ReadPreference;

public class MongoHandler<T extends IdentificableEntity>
    implements Handler<T>, InitializingBean {

    private MongoDaoFactory mongoDaoFactory;
    protected Class<T> clazz;
    public String collectionName;

    protected MongoDao<T> mongoDao;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.mongoDao = this.mongoDaoFactory.getInstance(this.collectionName, this.clazz);
    }

    public T get(final String id) {
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

    public Integer count(final HandlerQuery query) {
        if (query == null) {
            return this.mongoDao.getTotalObjectsInCollection(this.collectionName);
        }

        final MongoHandlerQuery mongoQuery = new MongoHandlerQuery(query);
        return this.mongoDao.getTotalObjectsInCollection(this.collectionName, mongoQuery.getQuery());
    }

    public String add(final T t) {
        t.setId(null);
        return this.mongoDao.insert(t);
    }

    @Override
    public String insertIfNotPresent(final T t) {
        return this.mongoDao.insert(t);
    }

    public String save(final T t) {
        return this.mongoDao.updateOrInsert(t).toString();
    }

    @Override
    public String update(final HandlerQuery query, final HandlerQuery updateQuery, boolean upsert) {
        final MongoHandlerQuery mongoQuery = new MongoHandlerQuery(query);
        final MongoHandlerQuery mongoUpdateQuery = new MongoHandlerQuery(updateQuery);
        Object[] res = (Object[]) this.mongoDao.update(mongoQuery.getQuery(), mongoUpdateQuery.getQuery(), upsert);
        if (res.length == 1) {
            return res[0].toString();
        }
        return null;
    }

    public void remove(final String id) {
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

    private ReadPreference isCrucialDataIntegration(HandlerQuery query) {
        if (query.isCrucialDataIntegration()) {
            return ReadPreference.PRIMARY;
        }

        return null;
    }
}
