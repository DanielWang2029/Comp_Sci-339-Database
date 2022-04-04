package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;
    private int iocpp;
    private DbFile dbf;
    private TupleDesc td;
    private DbFileIterator dbfiterator;
    private int fieldnum;
    private Object[] hists;
    private int tpcounter;
    private int pgcounter;
    private ArrayList<PageId> pids;
    private int[] fieldmins;
    private int[] fieldmaxs;
    private String[] strs;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

        this.iocpp = ioCostPerPage;
        this.dbf = Database.getCatalog().getDatabaseFile(tableid);
        this.td = this.dbf.getTupleDesc();
        this.dbfiterator = this.dbf.iterator(new TransactionId());
        this.fieldnum = this.td.numFields();
        this.hists = new Object[this.fieldnum];
        this.tpcounter = 0;
        this.pgcounter = 0;
        this.fieldmins = new int[this.fieldnum];
        this.fieldmaxs = new int[this.fieldnum];
        this.pids = new ArrayList<PageId>();
        this.strs = new String[this.fieldnum];


        try {
            for (int i = 0; i < this.fieldnum; i++) {
                this.fieldmins[i] = Integer.MAX_VALUE;
                this.fieldmaxs[i] = Integer.MIN_VALUE;
            }

            this.dbfiterator.open();

            while(this.dbfiterator.hasNext()){
                Tuple tuple = this.dbfiterator.next();
                for (int i = 0; i < this.fieldnum; i++) {
                    Type type = this.td.getFieldType(i);
                    if (type != Type.INT_TYPE && type != Type.STRING_TYPE) {
                        System.out.println("Error occurs with unknown type.");
                    } else if (type == Type.STRING_TYPE) {
                        StringField fd = (StringField) tuple.getField(i);
                        // do sth
                        this.strs[i] = fd != null ? fd.getValue() : "";
                    } else {
                        IntField fd = (IntField) tuple.getField(i);
                        this.fieldmins[i] = fd != null ? Math.min(this.fieldmins[i], fd.getValue()) : this.fieldmins[i];
                        this.fieldmaxs[i] = fd != null ? Math.max(this.fieldmaxs[i], fd.getValue()) : this.fieldmaxs[i];
                    }
                }

                this.tpcounter++;
                PageId pid = tuple.getRecordId().getPageId();
                if (!pids.contains(pid)) {
                    pids.add(pid);
                    this.pgcounter++;
                }
            }

            for (int i = 0; i < this.fieldnum; i++) {
                Type type = this.td.getFieldType(i);
                if (type != Type.INT_TYPE && type != Type.STRING_TYPE) {
                    System.out.println("Error occurs with unknown type.");
                } else if (type == Type.STRING_TYPE) {
                    this.hists[i] = new StringHistogram(NUM_HIST_BINS);
                } else {
                    this.hists[i] = new IntHistogram(NUM_HIST_BINS, this.fieldmins[i], this.fieldmaxs[i]);
                }
            }

            this.dbfiterator.rewind();

            while(this.dbfiterator.hasNext()){
                Tuple tuple = this.dbfiterator.next();
                for (int i = 0; i < this.fieldnum; i++) {
                    Type type = this.td.getFieldType(i);
                    if (type != Type.INT_TYPE && type != Type.STRING_TYPE) {
                        System.out.println("Error occurs with unknown type.");
                    } else if (type == Type.STRING_TYPE) {
                        StringField fd = (StringField) tuple.getField(i);
                        if (fd != null) {
                            ((StringHistogram) this.hists[i]).addValue(fd.getValue());
                        }
                    } else {
                        IntField fd = (IntField) tuple.getField(i);
                        if (fd != null) {
                            ((IntHistogram) this.hists[i]).addValue(fd.getValue());
                        }
                    }
                }
            }

            this.dbfiterator.close();

        } catch (DbException err) {
            System.out.println("DbException occurs.");
        } catch (TransactionAbortedException err) {
            System.out.println("TransactionAbortedException occurs");
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.iocpp * this.pgcounter;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) Math.ceil(this.tpcounter * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        double count = 0.0;
        Type type = this.td.getFieldType(field);
        for (int i = 0; i <= this.fieldmaxs[field] - this.fieldmins[field]; i++) {
            if (type != Type.INT_TYPE && type != Type.STRING_TYPE) {
                System.out.println("Error occurs with unknown type.");
            } else if (type == Type.STRING_TYPE) {
                StringField fd = new StringField(this.strs[field], this.strs[field].length());
                count += ((StringHistogram) this.hists[field]).estimateSelectivity(op, fd.getValue());
            } else {
                IntField fd = new IntField(i + this.fieldmins[i]);
                count +=  ((IntHistogram) this.hists[field]).estimateSelectivity(op, fd.getValue());
            }
        }
        return count / (this.fieldmaxs[field] - this.fieldmins[field] + 1);
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        try {
            Type type = this.td.getFieldType(field);
            if (type != Type.INT_TYPE && type != Type.STRING_TYPE) {
                System.out.println("Error occurs with unknown type.");
            } else if (type == Type.STRING_TYPE) {
                StringField fd = (StringField) constant;
                return ((StringHistogram) this.hists[field]).estimateSelectivity(op, fd.getValue());
            } else {
                IntField fd = (IntField) constant;
                return ((IntHistogram) this.hists[field]).estimateSelectivity(op, fd.getValue());
            }
        } catch (Exception err) {
            System.out.println("Error Occurs.");
        }
        return -1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.tpcounter;
    }

}
