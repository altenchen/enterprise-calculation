package storm.system;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author: xzp
 * @date: 2018-06-15
 * @description:
 */
public final class VehicleMap<T> {

    private final Map<String, T> map = new TreeMap<>();

    public int size() {
        return map.size();
    }

    @Contract(pure = true)
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Contract(pure = true)
    public boolean containsVehicle(String vid) {
        return map.containsKey(vid);
    }

    public T get(String vid) {
        return map.get(vid);
    }

    @Nullable
    public T put(String vid, T value) {
        return map.put(vid, value);
    }

    public T remove(String vid) {
        return map.remove(vid);
    }

    public void putAll(@NotNull VehicleMap<? extends T> vehicles) {
        this.map.putAll(vehicles.map);
    }

    public void clear() {
        map.clear();
    }

    @Contract(pure = true)
    @NotNull
    public Set<String> vehicleSet() {
        return map.keySet();
    }

    @Contract(pure = true)
    @NotNull
    public Collection<T> values() {
        return map.values();
    }

    @NotNull
    public Map<String, T> toMap() {
        final Map<String, T> result = new TreeMap<>();
        result.putAll(map);
        return result;
    }

    public void pushTo(@NotNull Map<String, T> output) {
        output.putAll(map);
    }

    public void pullFrom(@NotNull Map<String, T> input) {
        map.putAll(input);
    }
}
