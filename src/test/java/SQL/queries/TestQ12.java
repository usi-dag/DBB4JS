package SQL.queries;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ12 implements TestQ {
    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query12().execute(tpchStreamDB);
        var imperative = new Query12Imperative().execute(tpchStreamDB);

        var partialResult = new Query12SqlGroupBy().execute(tpchStreamDB);
        var singleFilter = new Query12SingleFilter().execute(tpchStreamDB);
        var singleFilterJoinMapMulti = new Query12SingleFilterAndJoinMapMulti().execute(tpchStreamDB);
        var singleFilterJoinMapMultiPartialResult = new Query12SingleFilterAndJoinMapMultiAndSqlGroupBy().execute(tpchStreamDB);
        var partialResultJoinMapMulti = new Query12SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);
        var partialResultSingleFilter = new Query12SqlGroupByAndSingleFilter().execute(tpchStreamDB);

        var joinMapMulti = new Query12JoinMapMulti().execute(tpchStreamDB);


        int size = stream.size();
        // check size
        assertEquals(size, imperative.size());
        assertEquals(size, partialResult.size());
        assertEquals(size, singleFilter.size());
        assertEquals(size, singleFilterJoinMapMulti.size());
        assertEquals(size, singleFilterJoinMapMultiPartialResult.size());
        assertEquals(size, partialResultJoinMapMulti.size());
        assertEquals(size, partialResultSingleFilter.size());
        assertEquals(size, joinMapMulti.size());

        var s = stream.toString();
        // check entires
        assertEquals(s, imperative.toString());
        assertEquals(s, partialResult.toString());
        assertEquals(s, singleFilter.toString());
        assertEquals(s, singleFilterJoinMapMulti.toString());
        assertEquals(s, singleFilterJoinMapMultiPartialResult.toString());
        assertEquals(s, partialResultJoinMapMulti.toString());
        assertEquals(s, partialResultSingleFilter.toString());
        assertEquals(s, joinMapMulti.toString());

    }
}
