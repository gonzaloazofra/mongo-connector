package com.despegar.integration.mongo.support;

import java.util.List;

import org.springframework.beans.factory.InitializingBean;

import com.google.common.collect.Lists;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MongoDBConnection
    implements InitializingBean {

    private Mongo mongo;
    private DB db;
    private List<ServerAddress> serverAddresses;
    private String replicaSet;
    private String dbName;

    @Override
    public void afterPropertiesSet() throws Exception {
        String[] addresses = this.replicaSet.split(",");
        ServerAddress serverAddress;
        for (String address : addresses) {
            String[] split = address.split(":");
            serverAddress = new ServerAddress(split[0], new Integer(split[1]));
            this.getServerAddresses().add(serverAddress);
        }
        this.mongo = new MongoClient(this.serverAddresses);
        this.db = this.mongo.getDB(this.dbName);
    }

    public Mongo getMongo() {
        return this.mongo;
    }

    public void setMongo(Mongo mongo) {
        this.mongo = mongo;
    }

    public String getDbName() {
        return this.dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public DB getDb() {
        return this.db;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    public String getReplicaSet() {
        return this.replicaSet;
    }

    public void setReplicaSet(String replicaSet) {
        this.replicaSet = replicaSet;
    }

    public List<ServerAddress> getServerAddresses() {
        if (this.serverAddresses == null) {
            this.serverAddresses = Lists.newArrayList();
        }
        return this.serverAddresses;
    }

    public void setServerAddresses(List<ServerAddress> serverAddresses) {
        this.serverAddresses = serverAddresses;
    }

}
