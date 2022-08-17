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

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
//        throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    //TODO 异常处理都统一归为 IllegalArgumentException 这个地方不确定是否正常
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPageId heapPageId = (HeapPageId) pid;
        RandomAccessFile rf;
        HeapPage hp;
        try {
            rf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException();
        }

        int size = BufferPool.getPageSize();
        byte[] input = new byte[size];
        try {
            rf.read(input, heapPageId.getPageNumber() * size, size);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
        try {
            hp = new HeapPage(heapPageId, input);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
        return  hp;
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
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    class HeapFileIterator implements DbFileIterator {
        HeapFile f;
        TransactionId tid;
        Iterator<Tuple> it =null;
        HeapPage curPage = null;

        public HeapFileIterator(HeapFile f, TransactionId tid) {
            this.f = f;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId heapPageId = new HeapPageId(f.getId(),0);
            curPage = (HeapPage) Database.getBufferPool()
                    .getPage(tid, heapPageId, Permissions.READ_ONLY);
            it = curPage.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            int pageMaxNo = numPages() - 1;
            // may not open
            if (it == null) return false;
            if (it.hasNext()) return true;
            if (pageMaxNo > curPage.getId().getPageNumber()) {
                HeapPageId nextHeapPageId = new HeapPageId(f.getId(),
                        curPage.getId().getPageNumber() + 1);
                HeapPage nextCurPage = (HeapPage) Database.getBufferPool()
                        .getPage(tid, nextHeapPageId, Permissions.READ_ONLY);
                it = nextCurPage.iterator();
                return it.hasNext();
            }

            return  false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null) throw new NoSuchElementException();
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
            curPage = null;
        }
    }

}

