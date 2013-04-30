package com.despegar.integration.mongo.entities;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.despegar.integration.domain.api.IdentificableEntity;

/**
 * @author jformoso
 * 
 * Generic class to use when you don't want/need to type your response.
 * Beware that when you save it you'll end up with BOTH _id and id in mongo, but this has no adverse effects.
 * 
 */
public class GenericEntity
    implements IdentificableEntity, Map<String, Object>, Serializable {

    private static final long serialVersionUID = 7714845777140744565L;
    private Map<String, Object> map = new HashMap<String, Object>();

    @Override
    public String getId() {
        return this.map.containsKey("id") ? this.map.get("id").toString() : null;
    }

    @Override
    public void setId(String id) {
        this.map.put("id", id);
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return this.map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return this.map.containsValue(value);
    }

    @Override
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        return this.map.entrySet();
    }

    @Override
    public Object get(Object key) {
        return this.map.get(key);
    }

    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }


    @Override
    public Set<String> keySet() {
        return this.map.keySet();
    }

    @Override
    public Object put(String key, Object value) {
        if (key.equals("_id")) {
            key = "id";
        }
        return this.map.put(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> range) {
        this.map.putAll(range);

    }

    @Override
    public Object remove(Object key) {
        return this.map.remove(key);
    }

    @Override
    public int size() {
        return this.map.size();
    }

    @Override
    public Collection<Object> values() {
        return this.map.values();
    }
}
