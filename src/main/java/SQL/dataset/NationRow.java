package SQL.dataset;

public interface NationRow {
    int n_nationkey();
    String n_name();
    int n_regionkey();
    String n_comment();

    record Record(int n_nationkey, String n_name, int n_regionkey, String n_comment) implements NationRow {}

}
