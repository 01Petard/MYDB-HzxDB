package top.guoziyang.mydb.backend.dm;

import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.PageOne;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.tm.TransactionManager;

public interface DataManager {
    /**
     * 创建一个数据管理器实例
     * @param path 数据管理器文件路径
     * @param mem 数据管理器内存大小
     * @param tm 事务管理器
     * @return 数据管理器实例
     */
    static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();

        return dm;
    }

    /**
     * 打开一个数据管理器实例
     * @param path 数据管理器文件路径
     * @param mem 数据管理器内存大小
     * @param tm 事务管理器
     * @return 数据管理器实例
     */
    static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 保证在系统崩溃后恢复数据一致性
        if (!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        // 获取所有页面并填充 PageIndex
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }

    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();
}
