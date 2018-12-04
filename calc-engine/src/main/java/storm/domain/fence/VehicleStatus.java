package storm.domain.fence;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.area.AreaSide;

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

    @NotNull
    private AreaSide areaSide = AreaSide.UNKNOWN;

    public VehicleStatus(
        @NotNull final String vehicleId) {

        this.vehicleId = vehicleId;
    }

    @Contract(pure = true)
    public AreaSide getAreaSide() {
        return areaSide;
    }

    public void setAreaSide(@NotNull final AreaSide areaSide) {
        this.areaSide = areaSide;
    }
}
