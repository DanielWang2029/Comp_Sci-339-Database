package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
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

    public ArrayList<TDItem> TDList;
//    public Type[] typeAr;
//    public String[] fieldAr;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return TDList.iterator();
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
//        this.typeAr = typeAr.clone();
//        this.fieldAr = fieldAr.clone();
        TDList = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++) {
            TDList.add(new TDItem(typeAr[i], fieldAr[i]));
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
//        this.typeAr = typeAr.clone();
//        this.fieldAr = null;
        TDList = new ArrayList<TDItem>();
        for (Type type : typeAr) {
            TDList.add(new TDItem(type, null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDList.size();
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
        try {
            return TDList.get(i).fieldName;
        }
        catch(IndexOutOfBoundsException e) {
            System.out.println("yeet yaw");
            throw new NoSuchElementException("Invalid index");
        }
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
        try {
            return TDList.get(i).fieldType;
        }
        catch(IndexOutOfBoundsException e) {
            System.out.println("yeet yaw");
            throw new NoSuchElementException("Invalid index");
        }
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
        int index = -1;
        for (int i = 0; i < TDList.size(); i++) {
            if (name.equals(TDList.get(i).fieldName)) {
                index = i;
                break;
            }
        }
        if(index != -1) {
            return index;
        }
        else {
            throw new NoSuchElementException("ding dong your inquiry is wrong");
        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for(TDItem i : TDList) {
            size += i.fieldType.getLen();
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
        ArrayList<Type> mergeTypes = new ArrayList<>();
        ArrayList<String> mergeNames = new ArrayList<>();
        for(int i = 0; i < td1.numFields(); i++)
        {
            mergeTypes.add(td1.getFieldType(i));
            mergeNames.add(td1.getFieldName(i));
        }
        for(int i = 0; i < td2.numFields(); i++)
        {
            mergeTypes.add(td2.getFieldType(i));
            mergeNames.add(td2.getFieldName(i));
        }
        Type[] mergeTypeArray = new Type[mergeTypes.size()];
        mergeTypeArray = mergeTypes.toArray(mergeTypeArray);
        String[] mergeNameArray = new String[mergeNames.size()];
        mergeNameArray = mergeNames.toArray(mergeNameArray);
        return new TupleDesc(mergeTypeArray, mergeNameArray);
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
        if (!(o instanceof TupleDesc td))
            return false;
        if (this.numFields() == td.numFields()) {
            for (int i = 0; i < this.numFields(); i++){
                if (!this.getFieldType(i).equals(td.getFieldType(i)))
                    return false;
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return this.toString().hashCode();
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
        //what kind of idiot made strings not inherently appendable
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < this.numFields(); i++) {
            stringBuilder.append(TDList.get(i).fieldType).append("[").append(i).append("]").append("(").append(TDList.get(i).fieldName).append("), ");
        }
        return stringBuilder.toString();
    }
}
