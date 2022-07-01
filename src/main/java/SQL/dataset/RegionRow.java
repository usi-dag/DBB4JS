package SQL.dataset;

public interface RegionRow {
    int r_regionkey();

    String r_name();

    String r_comment();

    record Record(int r_regionkey, String r_name, String r_comment) implements RegionRow {
    }
}
