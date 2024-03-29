package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final Random random = new Random();
    final int numPages;   // number of pages -- currently, not enforced
    final ConcurrentMap<PageId, Page> pages; // hash table storing current pages in memory
    public LockManager lm;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>();
        this.lm = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }


    /**
     * Lock class
     */
    public class Lock{
        public boolean exclusive;
        public TransactionId tid;
        public Lock(boolean exclusive, TransactionId tid) {
            this.exclusive = exclusive;
            this.tid = tid;
        }
    }


    /**
     * Lock manager class
     */
    public class LockManager{

        public HashMap<PageId, ArrayList<Lock>> requests;
        public HashMap<TransactionId, ArrayList<PageId>> tidpgid;

        public LockManager() {
            requests = new HashMap<PageId, ArrayList<Lock>>();
            tidpgid = new HashMap<TransactionId, ArrayList<PageId>>();
        }

        public synchronized boolean addLock(boolean exclusive, PageId pgid, TransactionId tid) {
//            System.out.println("addlock 1");
            Lock toadd = new Lock(exclusive, tid);
            if (!this.requests.containsKey(pgid)) {
                this.requests.put(pgid, new ArrayList<Lock>());
            }
            requests.get(pgid).add(toadd);

            if (!this.tidpgid.containsKey(tid)) {
                this.tidpgid.put(tid, new ArrayList<PageId>());
            }
            tidpgid.get(tid).add(pgid);

            if (exclusive) {
                if (this.requests.get(pgid).get(0).tid.equals(tid)
                        && this.requests.get(pgid).get(0).exclusive) {
                    return true;
                } else {
                    if (this.requests.get(pgid).get(0).tid.equals(tid)
                            && !this.requests.get(pgid).get(0).exclusive
                            && this.requests.get(pgid).get(1).tid.equals(tid)) {
                        this.requests.get(pgid).remove(0);
                        return true;
                    } else {
                        return false;
                    }
                }
            } else {
                return !this.requests.get(pgid).get(0).exclusive;
            }
        }

        public synchronized void unlock(PageId pgid, TransactionId tid) {
//            System.out.println("unlock 1");
            if (!this.requests.containsKey(pgid)) { return; }
            if (this.requests.get(pgid).get(0).exclusive
                    && !this.requests.get(pgid).get(0).tid.equals(tid)) { return; }
            for (Lock lock : this.requests.get(pgid)) {
                if (lock.tid.equals(tid)) {
                    this.requests.get(pgid).remove(lock);
                    this.tidpgid.get(tid).remove(pgid);
                    break;
                }
            }
        }

        public boolean locked(PageId pgid, TransactionId tid) {
//            System.out.println("locked 1");
            if (!this.requests.containsKey(pgid)) { return false; }
            if (this.requests.get(pgid).get(0).exclusive) {
                return this.requests.get(pgid).get(0).tid.equals(tid);
            } else {
                for (Lock lock : this.requests.get(pgid)) {
                    if (lock.tid.equals(tid) && !lock.exclusive) { return true; }
                }
                return false;
            }
        }
    }


    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // XXX Yuan points out that HashMap is not synchronized, so this is buggy.
        // XXX TODO(ghuo): do we really know enough to implement NO STEAL here?
        //     won't we still evict pages?

//        lm.addLock(perm != Permissions.READ_ONLY, pid, tid);

        boolean set = true;
        while(set) {
            if (perm == Permissions.READ_ONLY) {
                set = !lm.addLock(false, pid, tid);
            } else {
                set = !lm.addLock(true, pid, tid);
            }
        }

        Page p;
        synchronized(this) {
        	p = pages.get(pid); 
            if(p == null) {
                if(pages.size() >= numPages) {
                    evictPage();
                }
                
                p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                pages.put(pid, p);
            }
        }
        return p;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        lm.unlock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lm.locked(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        if(commit)
        {
            try {
                flushAllPages();
                for(PageId pgid : this.lm.tidpgid.get(tid)) {
                    if (this.pages.containsKey(pgid)) {lm.unlock(pgid, tid);}
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //release all locks
        else
        {
            for(PageId pgid : this.lm.tidpgid.get(tid)) {
                if (this.pages.containsKey(pgid)) {lm.unlock(pgid, tid);}
            }
        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        // let the specific implementation of the file decide which page to add it
        // to.

        List<Page> dirtypages = file.insertTuple(tid, t);

        synchronized(this) {
            for (Page p : dirtypages){
                p.markDirty(true, tid);
                
                //System.out.println("ADDING TUPLE TO PAGE " + p.getId().pageno() + " WITH HASH CODE " + p.getId().hashCode());
                
                // if page in pool already, done.
                if(pages.get(p.getId()) != null) {
                    //replace old page with new one in case addTuple returns a new copy of the page
                    pages.put(p.getId(), p);
                }
                else {
                    
                    // put page in pool
                    if(pages.size() >= numPages)
                        evictPage();
                    pages.put(p.getId(), p);
                }
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> dirtypages = file.deleteTuple(tid, t);

        synchronized(this) {
        	for (Page p : dirtypages){
        		p.markDirty(true, tid);
                    
        		// if page in pool already, done.
        		if(pages.get(p.getId()) != null) {
        			//replace old page with new one in case deleteTuple returns a new copy of the page
        			pages.put(p.getId(), p);
                }
        		else {
                        
        			// put page in pool
        			if(pages.size() >= numPages)
        				evictPage();
                    pages.put(p.getId(), p);
                }	
        	}   
        }    
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        /* calls flushPage() for each page in the BufferPool */
        for (PageId pageId : pages.keySet()) flushPage(pageId);

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        Page p = pages.get(pid);
        if (p != null) {
            pages.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        
        Page p = pages.get(pid);
        if (p == null)
            return; //not in buffer pool -- doesn't need to be flushed

        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(p);
        p.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        for (PageId pgid : lm.tidpgid.get(tid)) {
            flushPage(pgid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // pick a random page and flush it.
        // XXX this will work for lab1, but not for lab4.
        // XXX this can cause pages to be evicted that have uncommitted updates on them
        Object[] pids = pages.keySet().toArray();
        PageId pid = (PageId) pids[random.nextInt(pids.length)];
        try {
            Page p = pages.get(pid);
            if (p.isDirty() != null) { //if this is dirty, remove first non-dirty
                boolean gotNew = false;
                for (PageId pg : pages.keySet()) {
                    if (pages.get(pg).isDirty() == null) {
                        pid = pg;
                        gotNew = true;
                        break;
                    }
                }
                if (!gotNew) {
                    throw new DbException("All buffer pool slots contain dirty pages;  COMMIT or ROLLBACK to continue.");
                }
            }
            //XXX: The above code makes sure page is not dirty. 
            //Assuming we have FORCE, Why do we flush it to disk?
            //Answer: yes we don't need this if we have FORCE, but we do need it if we don't.
            //it doesn't hurt to keep it here.            
            flushPage(pid);
        } catch (IOException e) {
            throw new DbException("could not evict page");
        }
        pages.remove(pid);
    }

}
