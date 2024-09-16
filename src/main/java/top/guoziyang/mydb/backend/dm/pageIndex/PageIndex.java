package top.guoziyang.mydb.backend.dm.pageIndex;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;  // 每个区间的大小，默认0.2KB

    private final Lock lock;
    private final List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取页面（算出区间号，直接取即可）
     * @param spaceSize
     * @return  PageInfo，其中包含页号和空闲空间大小的信息
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) number++;
            while (number <= INTERVALS_NO) {
                if (lists[number].isEmpty()) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
