package top.guoziyang.mydb.backend.dm;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.dataItem.DataItemImpl;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageOne;
import top.guoziyang.mydb.backend.dm.page.PageX;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.dm.pageIndex.PageIndex;
import top.guoziyang.mydb.backend.dm.pageIndex.PageInfo;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Types;
import top.guoziyang.mydb.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    /**
     * 读取数据。
     * 根据 UID 从缓存中获取 DataItem，并校验有效位
     * @param uid 索引的id，即页号
     * @return
     * @throws Exception
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    /**
     * 插入数据。
     * 1、在 pageIndex 中获取一个足以存储插入内容的页面的页号；
     * 2、获取页面后，写入插入日志；
     * 3、通过 pageX 插入数据，并返回插入位置的偏移；
     * 4、将页面信息重新插入 pageIndex
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        for (int i = 0; i < 5; i++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if (pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if (pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    /**
     * 从 key 中解析出页号，从 pageCache 中获取到页面，再根据偏移，解析出 DataItem
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    /**
     * 缓存释放，需要将 DataItem 写回数据源，
     * 由于对文件的读写是以页为单位的，因此只需将 DataItem 所在的页 release 即可
     * @param di
     */
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    /**
     * 在创建文件时初始化PageOne
     */
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    /**
     * 在打开已有文件时读入PageOne，并验证正确性
     * @return
     */
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 初始化pageIndex
     */
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            try {
                Page pg = pc.getPage(i);
                pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
                pg.release();
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
    }

}
