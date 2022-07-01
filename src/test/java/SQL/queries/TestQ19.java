package SQL.queries;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ19 implements TestQ {

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query19().execute(tpchStreamDB);
        var imperative = new Query19Imperative().execute(tpchStreamDB);
        var joinMapMulti = new Query19JoinMapMulti().execute(tpchStreamDB);
        var singleFilter = new Query19SingleFilter().execute(tpchStreamDB);
        var singleFilterSqlGroubBy = new Query19SingleFilterAndJoinMapMulti().execute(tpchStreamDB);

        int size = stream.size();
        // check size
        assertEquals(size, imperative.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, singleFilter.size());
        assertEquals(size, singleFilterSqlGroubBy.size());

        var s = stream.toString();
        try {
            assertEquals(s, joinMapMulti.toString());
        } catch (AssertionFailedError e){
            for (int i = 0; i < size; i++) {
                var a = (Query19.Result) stream.get(i);
                var b = (Query19.Result) joinMapMulti.get(i);
                assertEquals(a.revenue, b.revenue, 0.0001f);
            }
        }
        // check entires
        try {
            assertEquals(s, imperative.toString());

        } catch (AssertionFailedError e) {
            for (int i = 0; i < size; i++) {
                var a = (Query19.Result) stream.get(i);
                var b = (Query19Imperative.Result) imperative.get(i);
                assertEquals(a.revenue, b.revenue(), 0.0001f);
            }
        }

        assertEquals(s, singleFilter.toString());
        assertEquals(s, singleFilterSqlGroubBy.toString());

    }

}
