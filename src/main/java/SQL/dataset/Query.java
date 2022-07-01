package SQL.dataset;

import java.util.List;

public interface Query {
    List<?> execute(TPCHStreamDB db);

}

