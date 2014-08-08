package com.despegar.integration.mongo.support;

import java.net.UnknownHostException;

import org.springframework.beans.factory.InitializingBean;

import com.despegar.integration.domain.api.GenericIdentificableEntity;
import com.despegar.integration.mongo.id.IdGenerator;
import com.despegar.integration.mongo.id.StringIdGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DB;

@SuppressWarnings("rawtypes")
public class MongoDaoFactory
    implements InitializingBean {

    private DB db;
    private MongoDBConnection mongoDBConnection;
    // dbName is not used anymore
    private String dbName;
    private IdGenerator idGenerator;

    private ObjectMapper mapper;

    public <T extends GenericIdentificableEntity<X>, X extends Object> MongoDao<T> getInstance(String collection,
        Class<T> clazz) {
        MongoDao<T> m = null;
        try {
            m = new MongoDao<T>(this.db, collection, this.mapper == null ? new ObjectMapper() : this.mapper, clazz,
                this.idGenerator == null ? new StringIdGenerator() : this.idGenerator);
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return m;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.db = this.mongoDBConnection.getDb();
    }

    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public void setIdGenerator(IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    public void setMongoDBConnection(MongoDBConnection mongoDBConnection) {
        this.mongoDBConnection = mongoDBConnection;
    }

    public MongoDBConnection getMongoDBConnection() {
        return this.mongoDBConnection;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

}
