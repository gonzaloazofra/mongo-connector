package com.despegar.integration.mongo.connector.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.OrderedMapIterator;

import com.despegar.integration.mongo.connector.HandlerQuery;
import com.despegar.integration.mongo.connector.HandlerQuery.ComparisonOperation;
import com.despegar.integration.mongo.connector.HandlerQuery.OperationWithComparison;
import com.despegar.integration.mongo.connector.HandlerQuery.OperationWithRange;
import com.despegar.integration.mongo.connector.HandlerQuery.OrderDirection;
import com.despegar.integration.mongo.connector.HandlerQuery.RangeOperation;
import com.despegar.integration.mongo.connector.Page;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class MongoHandlerQuery {

    private final static String ID_FIELD = "id";
    private final static String MONGO_ID_FIELD = "_id";

    private final HandlerQuery handlerQuery;

    public MongoHandlerQuery(final HandlerQuery handerQuery) {
        this.handlerQuery = handerQuery;
    }

    public BasicDBObject getQuery() {

        BasicDBObject res;

        if (this.handlerQuery.getOrs().isEmpty()) {
            res = this.createQueryFromHandler(this.handlerQuery);
        } else {
            final List<HandlerQuery> queries = new ArrayList<HandlerQuery>();
            queries.add(this.handlerQuery);
            queries.addAll(this.handlerQuery.getOrs());


            final BasicDBList orComponents = new BasicDBList();
            for (final HandlerQuery handlerQuery : queries) {
                orComponents.add(this.createQueryFromHandler(handlerQuery));
            }

            res = new BasicDBObject("$or", orComponents); // orRoot
        }

        return res;
    }


    private BasicDBObject createQueryFromHandler(final HandlerQuery query) {
        BasicDBObject dbQuery = new BasicDBObject();

        for (String key : query.getFilters().keySet()) {

            final Object pureValue = query.getFilters().get(key);
            final Object value = pureValue;

            if (ID_FIELD.equals(key)) {
                key = MONGO_ID_FIELD;
            }

            dbQuery.append(key, value);
        }

        dbQuery = this.prependUpsateOperation(query, dbQuery);

        this.appendRangeOperations(query, dbQuery);

        this.appendComparisionOperations(query, dbQuery);

        return dbQuery;
    }

    private BasicDBObject prependUpsateOperation(HandlerQuery query, BasicDBObject dbQuery) {
        if (query.getUpdateOperation() != null) {
            String updateOperation = null;
            switch (query.getUpdateOperation()) {
            case INC:
                updateOperation = "$inc";
                break;
            case SET:
                updateOperation = "$set";
                break;
            case UNSET:
                updateOperation = "$unset";
                break;
            case ADD_TO_SET:
                updateOperation = "$addToSet";
                break;
            case POP:
                updateOperation = "$pop";
                break;
            case PULL_ALL:
                updateOperation = "$pullAll";
                break;
            case PULL:
                updateOperation = "$pull";
                break;
            case PUSH:
                updateOperation = "$push";
                break;
            default:
                break;
            }
            if (updateOperation != null) {
                return new BasicDBObject(updateOperation, dbQuery);
            }
        }
        return dbQuery;
    }

    private BasicDBObject appendComparisionOperations(final HandlerQuery query, final BasicDBObject dbQuery) {
        final Set<Entry<String, OperationWithComparison>> comparison = query.getComparisonOperators().entrySet();

        for (final Entry<String, OperationWithComparison> entry : comparison) {
            final String key = entry.getKey();

            final List<OperationWithComparison> comparisions = new ArrayList<HandlerQuery.OperationWithComparison>();
            final OperationWithComparison operationWithComparision = entry.getValue();
            comparisions.add(operationWithComparision);

            comparisions.addAll(operationWithComparision.getMoreComparisions());

            final BasicDBObject comparisionComponents = new BasicDBObject();
            for (final OperationWithComparison eachOperation : comparisions) {
                comparisionComponents.append(this.getComparisonOperation(eachOperation.getOperation()),
                    eachOperation.getValue());
            }

            dbQuery.append(key, comparisionComponents);
        }

        return dbQuery;
    }

    private void appendRangeOperations(final HandlerQuery query, final BasicDBObject dbQuery) {
        final Set<Entry<String, OperationWithRange>> rangeOperations = query.getRangeOperators().entrySet();
        for (final Entry<String, OperationWithRange> entry : rangeOperations) {
            final String key = entry.getKey();
            final RangeOperation operation = entry.getValue().getCollectionOperation();
            final Collection<?> values = entry.getValue().getValues();

            final BasicDBList list = new BasicDBList();
            list.addAll(values);
            final DBObject inClouse = new BasicDBObject(this.getRangeOperation(operation), list);
            dbQuery.append(key, inClouse);
        }
    }

    private String getComparisonOperation(final ComparisonOperation operation) {

        if (HandlerQuery.ComparisonOperation.LESS.equals(operation)) {
            return "$lt";
        }
        if (HandlerQuery.ComparisonOperation.LESS_OR_EQUAL.equals(operation)) {
            return "$lte";
        }
        if (HandlerQuery.ComparisonOperation.GREATER.equals(operation)) {
            return "$gt";
        }
        if (HandlerQuery.ComparisonOperation.GREATER_OR_EQUAL.equals(operation)) {
            return "$gte";
        }
        if (HandlerQuery.ComparisonOperation.NOT_EQUAL.equals(operation)) {
            return "$ne";
        }
        return null;
    }

    private String getRangeOperation(final RangeOperation operation) {
        // no pongo estos valores en los enums para que no queden atados a MONGO, y que el enum sirva para cualquier
        // implementación

        if (HandlerQuery.RangeOperation.IN.equals(operation)) {
            return "$in";
        }

        if (HandlerQuery.RangeOperation.NOT_IN.equals(operation)) {
            return "$nin";
        }

        if (HandlerQuery.RangeOperation.ALL.equals(operation)) {
            return "$all";
        }
        return null;
    }

    public DBObject getSortInfo() {
        if (!this.handlerQuery.getOrderFields().isEmpty()) {
            // use a set of fields and order directions0
            final DBObject sortInfo = new BasicDBObject(this.handlerQuery.getOrderFields().size());

            final OrderedMapIterator orderedMapIterator = this.handlerQuery.getOrderFields().orderedMapIterator();
            while (orderedMapIterator.hasNext()) {
                final String key = (String) orderedMapIterator.next();
                final OrderDirection orderDir = (OrderDirection) orderedMapIterator.getValue();
                sortInfo.put(key, this.getOrderDir(orderDir));
            }

            return sortInfo;

        }
        return null;

    }

    public Page getPage() {
        return this.handlerQuery.getPage();
    }

    // no pongo los valores en el enum para no acoplar estos valores de mongo a un enum genérico
    private int getOrderDir(final OrderDirection orderDir) {

        if (HandlerQuery.OrderDirection.ASC.equals(orderDir)) {
            return 1;
        }

        if (HandlerQuery.OrderDirection.DESC.equals(orderDir)) {
            return -1;
        }

        return 0;
    }

}
