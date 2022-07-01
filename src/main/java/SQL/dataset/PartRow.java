package SQL.dataset;

public interface PartRow {
    long p_partkey();

    String p_name();

    String p_mfgr();

    String p_brand();

    String p_type();

    int p_size();

    String p_container();

    double p_retailprice();

    String p_comment();

    record Record(long p_partkey, String p_name, String p_mfgr, String p_brand,
                  String p_type, int p_size, String p_container, double p_retailprice,
                  String p_comment) implements PartRow {
    }


}
