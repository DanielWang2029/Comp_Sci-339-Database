package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
//how to get iterator without file? might as well import file
import simpledb.storage.DbFile;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFile file;
    private DbFileIterator iter;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.iter = file.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        // didn't say some code goes here originally, but I assume that is typo
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        //if reset, we change the file and iter too?
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.iter = file.iterator(tid);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }
    //by importing dbfile and using the file for the iterator, this should be much easier
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        iter.open();

    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc data = Database.getCatalog().getTupleDesc(tableid);
        int arrsize = data.numFields();
        //tupledesc needs types and fields
        //merge in td was better for arraylists, but since we already know the tdlist, we might as well just use array
        Type[] types = new Type[arrsize];
        String[] fields = new String[arrsize];
        for(int i = 0; i < arrsize; i++)
        {
            types[i] = data.getFieldType(i);
            StringBuilder sb = new StringBuilder();
            //no idea what valueOf has to deal w the fact that the sb.append should be a string
            //I miss python
            fields[i] = String.valueOf(sb.append(tableAlias).append(".").append(data.getFieldName(i)));
        }
        return new TupleDesc(types, fields);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return iter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return iter.next();
    }

    public void close() {
        // some code goes here
        iter.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        iter.rewind();
    }
}
