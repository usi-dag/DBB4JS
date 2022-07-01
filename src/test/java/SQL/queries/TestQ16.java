package SQL.queries;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ16 implements TestQ{


    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query16().execute(tpchStreamDB);
        var imperative = new Query16Imperative().execute(tpchStreamDB);

        var joinMapMulti = new Query16JoinMapMulti().execute(tpchStreamDB);
        var singleFilter = new Query16SingleFilter().execute(tpchStreamDB);
        var partialResult = new Query16SqlGroupBy().execute(tpchStreamDB);

        var partialResultJoinMapMulti = new Query16SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);
        var partialResultJoinMapMultiSingleFilter = new Query16SqlGroupByAndJoinMapMultiAndSingleFilter().execute(tpchStreamDB);
        var singleFilterPartialResult = new Query16SingleFilterAndSqlGroupBy().execute(tpchStreamDB);
        var singleFilterJoinMapMulti = new Query16SingleFilterAndJoinMapMulti().execute(tpchStreamDB);

        int size = stream.size();
        // check size
        assertEquals(size, imperative.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, singleFilter.size());
        assertEquals(size, partialResult.size());
        assertEquals(size, partialResultJoinMapMulti.size());
        assertEquals(size, partialResultJoinMapMultiSingleFilter.size());
        assertEquals(size, singleFilterPartialResult.size());
        assertEquals(size, singleFilterJoinMapMulti.size());

        var s = stream.toString();
        assertEquals(s, imperative.toString());
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, singleFilter.toString());
        assertEquals(s, partialResult.toString());

        assertEquals(s, partialResultJoinMapMulti.toString());
        assertEquals(s, partialResultJoinMapMultiSingleFilter.toString());
        assertEquals(s, singleFilterPartialResult.toString());
        assertEquals(s, singleFilterJoinMapMulti.toString());





    }
}
