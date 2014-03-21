package com.despegar.integration.mongo.connector;

import org.springframework.beans.factory.InitializingBean;

import com.despegar.integration.domain.api.IdentificableEntity;

/**
 *
 * @param <T>
 */

@Deprecated
public interface HandlerContainer<T extends IdentificableEntity>
    extends InitializingBean {

    public Handler<T> getHandler(String itemType);

}
