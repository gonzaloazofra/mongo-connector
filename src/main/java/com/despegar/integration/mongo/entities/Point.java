package com.despegar.integration.mongo.entities;

public class Point {

    private Double[] coordinates;
    private String type;

    public Point(Double[] coordinates) {
        this.coordinates = coordinates;
        this.type = "Point";
    }

    public Double[] getCoordinates() {
        return this.coordinates;
    }

    public void setCoordinates(Double[] coordinates) {
        this.coordinates = coordinates;
    }
}
