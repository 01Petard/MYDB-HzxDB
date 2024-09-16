package top.guoziyang.mydb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

/**
 * 实现了数据页
 */
public class PageImpl implements Page {
    /**
     * 数据页的页号，页号从 1 开始
     */
    private int pageNumber;
    /**
     * 这个页实际包含的字节数据
     */
    private byte[] data;
    /**
     * 标志着这个页面是否是脏页面，在缓存驱逐的时候，脏页面需要被写回磁盘
     */
    private boolean dirty;
    private Lock lock;
    /**
     * PageCache 方便在拿到 Page 的引用时释放这个页面的缓存
     */
    private PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void release() {
        pc.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }

}
