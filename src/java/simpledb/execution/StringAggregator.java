package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private String NOGROUPING = "NO_GROUPING";
    private GBHandler gbHandler;
    private int gbfield;
//    private int afield;
    private Type gbfieldtype;
    private Op what;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // 这里 的 afield 感觉没啥用，因为我们只 提供 count计算 我把 afield 从 构造函数中 删除掉。这样如果后面出问题容易发现
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        gbHandler = new CountAggregator();
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;


    }
    private abstract class GBHandler{
        ConcurrentHashMap<String,Integer> gbResult;
        abstract void handle(String key);
        private GBHandler(){
            gbResult = new ConcurrentHashMap<>();
        }
        public Map<String,Integer> getGbResult(){
            return gbResult;
        }
    }
    private  class  CountAggregator extends GBHandler {
        @Override
        void handle(String key) {
            if (gbResult.containsKey(key)){
                gbResult.put(key, gbResult.get(key) + 1);
            } else {
                gbResult.put(key, 1);
            }
        }
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String key;
        if(gbfield == NO_GROUPING){
            key = NOGROUPING;
        } else {
            key = tup.getField(gbfield).toString();
        }

        gbHandler.handle(key);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Type[] types;
        TupleDesc td;
        List<Tuple> list = new ArrayList<>();

        if(gbfield == NO_GROUPING) {
            gbHandler.getGbResult().get(NOGROUPING);
            types= new Type[]{Type.INT_TYPE};
            String[] names = new String[]{"aggregateVal"};
            td = new TupleDesc(types, names);
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(gbHandler.getGbResult().get(NOGROUPING)));
            list.add(tuple);
        } else {
            types= new Type[]{gbfieldtype, Type.INT_TYPE};
            String[] names = new String[]{"groupVal", "aggregateVal"};
            td = new TupleDesc(types, names);
            for (Map.Entry<String,Integer> entry :gbHandler.getGbResult().entrySet()) {
                Tuple tuple = new Tuple(td);

                if (gbfieldtype == Type.INT_TYPE){
                    tuple.setField(0, new IntField(Integer.parseInt(entry.getKey())));
                } else {
                    tuple.setField(0, new StringField(entry.getKey(), Type.STRING_LEN));
                }

                tuple.setField(1, new IntField(entry.getValue()));
                list.add(tuple);
            }
        }
        return new TupleIterator(td, list);

    }

}
