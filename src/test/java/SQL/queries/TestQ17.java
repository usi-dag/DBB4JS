package SQL.queries;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ17 implements TestQ{

    @Test
    @Override
    public void testCorrectness() {
        var stream =  new Query17().execute(tpchStreamDB);
        var imperative = new Query17Imperative().execute(tpchStreamDB);
        var partialResult = new Query17SqlGroupBy().execute(tpchStreamDB);
        var singleFilter = new Query17SingleFilter().execute(tpchStreamDB);
        var singleFilterPartialSum = new Query17SingleFilterAndSqlGroupBy().execute(tpchStreamDB);

        var streamValue = (Query17.Result) stream.get(0);
        var imperativeValue = (Query17.Result) imperative.get(0);
        var partialResultValue = (Query17.Result) partialResult.get(0);
        var singleFilterValue = (Query17.Result) singleFilter.get(0);
        var singleFilterPartialSumValue = (Query17.Result) singleFilterPartialSum.get(0);


        assertEquals(streamValue.avg_yearly(), imperativeValue.avg_yearly(), 0.00001f);
        assertEquals(streamValue.avg_yearly(), partialResultValue.avg_yearly(), 0.00001f);
        assertEquals(streamValue.avg_yearly(), singleFilterValue.avg_yearly(), 0.00001f);
        assertEquals(streamValue.avg_yearly(), singleFilterPartialSumValue.avg_yearly(), 0.00001f);


    }
}
