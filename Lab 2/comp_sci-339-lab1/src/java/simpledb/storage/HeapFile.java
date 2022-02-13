package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;
    private RandomAccessFile rafile;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        try {
            this.rafile = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("random access file creation failed");
        }

        // random access file? https://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html

    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.getFile().getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        //First we seek the file based off offset
        byte[] pagedata = HeapPage.createEmptyPageData();

        try{ //intellij says cast to long. computer smarter than me. long it goes.
            this.rafile.seek((long) BufferPool.getPageSize() * pid.getPageNumber());
            //now we get the data from the read
            for(int i = 0; i < pagedata.length; i++)
                pagedata[i] = rafile.readByte();
            //now we make the page
            System.out.println(pid.getTableId());
            System.out.println(pid.getPageNumber());
            return new HeapPage((HeapPageId) pid, pagedata);
        } catch (IOException e) {
            throw new RuntimeException("readpage is shitting itself, try again");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        try {
            return (int) (this.rafile.length() / BufferPool.getPageSize());
        } catch (IOException e) {
            throw new RuntimeException("BufferPool issue probably, L102");
        }
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        class Hfi implements DbFileIterator {
            private HeapFile hf;
            private TransactionId tid;
            private boolean isOpen;
            private Page currentPage;
            private int currentPageIndex;
            private PageId currentPageId;
            private Iterator<Tuple> currentPageIterator;

            public Hfi(HeapFile hf, TransactionId tid) {
                this.hf = hf;
                this.tid = tid;
                this.isOpen = false;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                try {
                    this.currentPageIndex = 0;
                    this.currentPageId = new HeapPageId(this.hf.getId(), this.currentPageIndex);
                    this.currentPage = Database.getBufferPool().getPage(this.tid, this.currentPageId, Permissions.READ_WRITE);
                    this.currentPageIterator = ((HeapPage) this.currentPage).iterator();
                    this.isOpen = true;
                } catch (Exception e) {
                    throw new DbException("");
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!this.isOpen) {return false;}
                try {
                    if (this.currentPageIterator.hasNext()) {return true;}
                    if (this.currentPageIndex < this.hf.numPages() - 1) {return true;}
                } catch (Exception e) {
                    throw new DbException("");
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!this.isOpen) {throw new NoSuchElementException("Calling next when closed");}
                try {
                    if (this.currentPageIterator.hasNext()) {return this.currentPageIterator.next();}
                    while ((!this.currentPageIterator.hasNext()) && this.currentPageIndex < this.hf.numPages() - 1) {
                        this.currentPageIndex++;
                        this.currentPageId = new HeapPageId(this.hf.getId(), this.currentPageIndex);
                        this.currentPage = Database.getBufferPool().getPage(this.tid, this.currentPageId, Permissions.READ_WRITE);
                        this.currentPageIterator = ((HeapPage) this.currentPage).iterator();
                    }
                    if (this.currentPageIterator.hasNext()) {return this.currentPageIterator.next();}
                    else {throw new NoSuchElementException();}
                } catch (Exception e) {
                    throw new DbException("");
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.close();
                this.open();
            }

            @Override
            public void close() {
                this.isOpen = false;
            }
        }

        return new Hfi(this, tid);
    }

}

