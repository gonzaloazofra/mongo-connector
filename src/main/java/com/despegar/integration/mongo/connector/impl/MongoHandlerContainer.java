package com.despegar.integration.mongo.connector.impl;

import java.util.List;
import java.util.Map;

import org.springframework.util.Assert;

import com.despegar.integration.domain.api.IdentificableEntity;
import com.despegar.integration.mongo.connector.Handler;
import com.despegar.integration.mongo.connector.HandlerContainer;
import com.despegar.integration.mongo.support.MongoDaoFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MongoHandlerContainer<T extends IdentificableEntity>
    implements HandlerContainer<T> {

    private Map<String, MongoHandler<T>> handlers;
    private List<String> reviewTypes;
    private String suffix;
    private Class<T> clazz;
    private MongoDaoFactory mongoDaoFactory;

    @Override
    public Handler<T> getHandler(final String itemType) {
        MongoHandler<T> handler = this.handlers.get(itemType.toLowerCase());
        Assert.notNull(handler, "Item type " + itemType + " not supported!");

        return handler;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.handlers = Maps.newHashMap();

        for (final String itemType : this.reviewTypes) {

            final MongoHandler<T> handler = new MongoHandler<T>();
            handler.setCollectionName(itemType + this.suffix);
            handler.setClazz(this.clazz);
            handler.setMongoDaoFactory(this.mongoDaoFactory);
            handler.afterPropertiesSet();

            this.handlers.put(itemType.toLowerCase(), handler);
        }


    }

    public void setReviewTypes(final String reviewTypes) {
        this.reviewTypes = Lists.newArrayList(reviewTypes.split(","));
    }

    public void setClazz(final Class<T> clazz) {
        this.clazz = clazz;
    }

    public void setMongoDaoFactory(final MongoDaoFactory mongoDaoFactory) {
        this.mongoDaoFactory = mongoDaoFactory;
    }

    public void setSuffix(final String suffix) {
        this.suffix = suffix;
    }
}
