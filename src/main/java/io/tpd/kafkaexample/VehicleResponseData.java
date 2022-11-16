package io.tpd.kafkaexample;

public record VehicleResponseData(Long id, Double traveledDistance) {

    public VehicleResponseData(Long id, Double traveledDistance) {
        this.id = id;
        this.traveledDistance = traveledDistance;
    }

}