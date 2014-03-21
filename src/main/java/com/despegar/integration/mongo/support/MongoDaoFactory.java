package com.despegar.integration.mongo.support;

import org.springframework.beans.factory.InitializingBean;

import com.despegar.integration.domain.api.GenericIdentificableEntity;
import com.despegar.integration.mongo.id.IdGenerator;
import com.despegar.integration.mongo.id.StringIdGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DB;
import com.mongodb.Mongo;

@SuppressWarnings("rawtypes")
public class MongoDaoFactory
    implements InitializingBean {

    private Mongo mongoDBConnection;
    private String dbName;
    private DB db;
    private IdGenerator idGenerator;

    private ObjectMapper mapper;

    public <T extends GenericIdentificableEntity<X>, X extends Object> MongoDao<T> getInstance(String collection,
        Class<T> clazz) {
        MongoDao<T> m = null;
        m = new MongoDao<T>(this.db, collection, this.mapper == null ? new ObjectMapper() : this.mapper, clazz,
            this.idGenerator == null ? new StringIdGenerator() : this.idGenerator);

        return m;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.db = this.getMongoDBConnection().getDB(this.dbName);
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public Mongo getMongoDBConnection() {
        return this.mongoDBConnection;
    }

    public void setMongoDBConnection(Mongo mongoDBConnection) {
        this.mongoDBConnection = mongoDBConnection;
    }

    public void setIdGenerator(IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

}
