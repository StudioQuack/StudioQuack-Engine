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
public class StyleDto implements Serializable, Cloneable {

    private double width;
    private double height;

    // Static factory method for convenience (Optional)
    public static StyleDto of(double x, double y) {
        return new StyleDto(x, y);
    }

    // Example method using log (if needed)
    public void logMeasurements() {
        log.debug("Style Measurements: width={}, height={}", this.width, this.height);
    }

    @Override
    public StyleDto clone() {
        try {
            return (StyleDto) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
