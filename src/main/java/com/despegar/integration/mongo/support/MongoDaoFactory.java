package com.despegar.integration.mongo.support;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.despegar.integration.domain.api.IdentificableEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DB;
import com.mongodb.Mongo;

public class MongoDaoFactory
    implements InitializingBean {

    private Mongo mongoDBConnection;
    private String dbName;
    private DB db;

    private ObjectMapper mapper;

    public <T extends IdentificableEntity> MongoDao<T> getInstance(String collection, Class<T> clazz) {
        MongoDao<T> m = null;
        if (this.mapper == null) {
            m = new MongoDao<T>(this.db, collection, clazz);
        } else {
            m = new MongoDao<T>(this.db, collection, this.mapper, clazz);
        }

        return m;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.db = this.mongoDBConnection.getDB(this.dbName);
    }

    @Autowired
    void setMongoDBConnection(Mongo mongoDBConnection) {
        this.mongoDBConnection = mongoDBConnection;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

}
