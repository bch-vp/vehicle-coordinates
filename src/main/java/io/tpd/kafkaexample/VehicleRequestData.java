package io.tpd.kafkaexample;

import static org.apache.commons.lang3.Validate.notBlank;

public record VehicleRequestData(Long id, Double x, Double y) {

    public VehicleRequestData(Long id, Double x, Double y) {
        this.id = id;
        this.x = x;
        this.y = y;
    }

}
