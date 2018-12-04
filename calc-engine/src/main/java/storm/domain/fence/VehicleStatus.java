package storm.domain.fence;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-12-04
 * @description:
 */
public final class VehicleStatus {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(VehicleStatus.class);

    @NotNull
    private final String vehicleId;

    public VehicleStatus(
        @NotNull final String vehicleId) {

        this.vehicleId = vehicleId;
    }
}
