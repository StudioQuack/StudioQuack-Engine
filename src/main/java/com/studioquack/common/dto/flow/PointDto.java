package com.studioquack.common.dto.flow;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

/**
 * Represents a simple X, Y coordinate.
 */
@Data
@Slf4j
@NoArgsConstructor 
@AllArgsConstructor
@EqualsAndHashCode
public class PointDto implements Serializable, Cloneable {
    private double x;
    private double y;

    // Static factory method for convenience (Optional)
    public static PointDto of(double x, double y) {
        return new PointDto(x, y);
    }

    // Example method using log (if needed)
    public void logCoordinates() {
        log.debug("Point coordinates: x={}, y={}", this.x, this.y);
    }

    @Override
    public PointDto clone() {
        try {
            return (PointDto) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
