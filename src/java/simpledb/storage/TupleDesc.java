package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

import static simpledb.execution.Aggregator.NO_GROUPING;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    List<TDItem> TDItemList;
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return TDItemList.iterator();
//        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if(typeAr.length < 1)return;
        TDItemList = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItemList.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if (typeAr.length < 1)return;
        TDItemList = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++){
            TDItemList.add(new TDItem(typeAr[i], null));
        }
    }

    public TupleDesc(TupleDesc in) {
        TDItemList = new ArrayList<>();
        Iterator<TDItem> inIter = in.iterator();
        while(inIter.hasNext()){
            TDItemList.add(inIter.next());
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDItemList.size();
//        return 0;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= getSize()){
            throw new NoSuchElementException();
        }
        Iterator<TDItem> tmp = iterator();
        while (tmp.hasNext() && i!=0){
            tmp.next();
            i--;
        }
        return tmp.next().fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        //这里特殊处理一下，针对 NO_GROUPING场景 TODO 待确认更好的做法
        if(i == NO_GROUPING) return Type.STRING_TYPE;

        if (i < 0 || i >= TDItemList.size()) {
            throw new NoSuchElementException();
        }
        Iterator<TDItem> tmp = iterator();
        while (tmp.hasNext() && i != 0){
            tmp.next();
            i--;
        }
        return tmp.next().fieldType;

//        return null;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name == null) throw  new NoSuchElementException();
        Iterator<TDItem> tmp = iterator();
        int i =0;
        boolean found = false;
        while (tmp.hasNext()){
            if(name.equals(tmp.next().fieldName)){
                found = true;
                break;
            }
            i++;
        }
        if(!found) throw  new NoSuchElementException();
        return i;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        Iterator<TDItem> tmp = iterator();
        while (tmp.hasNext()) {
            TDItem tdItem = tmp.next();
            size += tdItem.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TupleDesc td3 = new TupleDesc(td1);
        td3.TDItemList.addAll(td2.TDItemList);
        return td3;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(this == o){
            return true;
        }
        if(o == null){
            return false;
        }

        if(getClass() != o.getClass()){
            return false;
        }

        TupleDesc tupleDesc = (TupleDesc) o;
        Iterator<TDItem> A = this.iterator();
        Iterator<TDItem> B = tupleDesc.iterator();

        while (A.hasNext() && B.hasNext()){
            TDItem aitem = A.next();
            TDItem bitem = B.next();
            if (aitem.fieldType == bitem.fieldType) {
                if (aitem.fieldName == null && bitem.fieldName == null)continue;
                if (bitem.fieldName == null || aitem.fieldName == null) return false;
                if (!aitem.fieldName.equals(bitem.fieldName))return false;
            } else {
                return false;
            }
        }
        if ( A.hasNext() || B.hasNext()) return  false;


        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        Iterator<TDItem> tmp = iterator();
        int hashValue = 0;
        while (tmp.hasNext()) {
            TDItem tdItem = tmp.next();
            hashValue += tdItem.toString().hashCode();
        }

        return  hashValue;

//        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder result = new StringBuilder();
        Iterator<TDItem> tmp = iterator();
        while (tmp.hasNext()) {
            TDItem tdItem = tmp.next();
            result.append(tdItem.toString());
        }

        return result.toString();
    }
}
