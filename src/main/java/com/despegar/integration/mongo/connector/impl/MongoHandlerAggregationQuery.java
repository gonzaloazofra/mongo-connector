package com.despegar.integration.mongo.connector.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.util.Assert;

import com.despegar.integration.mongo.connector.HandlerAggregationQuery;
import com.despegar.integration.mongo.connector.HandlerAggregationQuery.Aggregation;
import com.despegar.integration.mongo.connector.HandlerAggregationQuery.AggregationOperation;
import com.despegar.integration.mongo.connector.HandlerAggregationQuery.GeometryAggregationSpecifier;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class MongoHandlerAggregationQuery {

    private final HandlerAggregationQuery handlerAggregationQuery;

    public MongoHandlerAggregationQuery(final HandlerAggregationQuery query) {
        this.handlerAggregationQuery = query;
    }

    public List<DBObject> getQuery() {
        List<DBObject> res = new ArrayList<DBObject>();
        List<Aggregation> aggregationList = this.handlerAggregationQuery.getAggregations();
        for (Aggregation aggregation : aggregationList) {
            res.add(this.appendAggregationOperations(aggregation));
        }
        return res;
    }

    private DBObject appendAggregationOperations(Aggregation aggregation) {
        DBObject dbQuery = new BasicDBObject();
        String key = this.getAggregationOperation(aggregation.getAggregationOperation());
        dbQuery.put(key, this.getAggregationQuery(aggregation));
        return dbQuery;
    }

    private DBObject getAggregationQuery(Aggregation aggregation) {
        Assert.notNull(
            AggregationOperation.GEO_NEAR.equals(aggregation.getAggregationOperation())
                && aggregation.getGeometrySpecifiers() == null, "Specifiers for geometry operations are required.");

        MongoHandlerQuery mongoHandlerQuery = new MongoHandlerQuery(aggregation.getQuery());

        if (aggregation.getGeometrySpecifiers() != null) {
            DBObject geometrySpecifiers = this.getGeometrySpecifiers(aggregation.getGeometrySpecifiers());
            geometrySpecifiers.put("query", mongoHandlerQuery.getQuery());
            return geometrySpecifiers;
        } else {
            return mongoHandlerQuery.getQuery();
        }
    }

    private DBObject getGeometrySpecifiers(GeometryAggregationSpecifier geometrySpecifiers) {
        DBObject specifierProperties = new BasicDBObject();
        specifierProperties.put("near", geometrySpecifiers.getNear());
        specifierProperties.put("distanceField", geometrySpecifiers.getDistanceField());
        specifierProperties.put("limit", geometrySpecifiers.getLimit());
        specifierProperties.put("num", geometrySpecifiers.getNum());
        specifierProperties.put("maxDistance", geometrySpecifiers.getMaxDistance());
        specifierProperties.put("spherical", geometrySpecifiers.isSpherical());
        specifierProperties.put("distanceMultiplier", geometrySpecifiers.getDistanceMultiplier());
        specifierProperties.put("includeLocs", geometrySpecifiers.getIncludeLocs());
        specifierProperties.put("uniqueDocs", geometrySpecifiers.isUniqueDocs());
        return specifierProperties;
    }

    private String getAggregationOperation(final AggregationOperation operation) {
        String aggregationOperation = null;
        switch (operation) {
        case GEO_NEAR:
            aggregationOperation = "$geoNear";
            break;
        case MATCH:
            aggregationOperation = "$match";
            break;
        case GROUP:
            aggregationOperation = "$group";
            break;
        }
        return aggregationOperation;
    }

}
