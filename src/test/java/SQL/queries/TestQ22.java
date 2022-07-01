package SQL.queries;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ22 implements TestQ{


    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query22().execute(tpchStreamDB);
        var imperative = new Query22Imperative().execute(tpchStreamDB);
        var singleFilter = new Query22SingleFilter().execute(tpchStreamDB);
        var sqlGroubBy = new Query22SqlGroubBy().execute(tpchStreamDB);
        var singleFilterSqlGroupBy = new Query22SingleFilterAndSqlGroupBy().execute(tpchStreamDB);

        var s =stream.toString();
        assertEquals(s, imperative.toString());
        assertEquals(s, singleFilter.toString());
        assertEquals(s, sqlGroubBy.toString());
        assertEquals(s, singleFilterSqlGroupBy.toString());
    }
}
