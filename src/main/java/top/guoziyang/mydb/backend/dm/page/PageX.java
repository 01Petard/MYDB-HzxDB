package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.Parser;

/**
 * 管理普通页，
 * 普通页结构：
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {
    /**
     * 页面空闲位置的偏移量
     */
    private static final short OF_FREE = 0;
    /**
     * 页面的数据位置的偏移量
     */
    private static final short OF_DATA = 2;
    /**
     * 获取空闲位置偏移，页面空闲大小 = 页面大小 - 空闲位置偏移
     */
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;
    /**
     * 初始化一个页面的字节数据
     * @return
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }
    /**
     * 设置FreeSpaceOffset，即页面的空闲位置的偏移量
     * @param raw    数据页
     * @param ofData 新的空闲位置偏移量
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    // 获取pg的FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }
    /**
     * 获取FreeSpaceOffset，即页面的空闲位置的偏移量
     * @param raw 数据页数据
     * @return 页面的空闲位置的偏移量
     */
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 将数据插入pg中，返回插入位置
     * @param pg
     * @param raw  插入的数据
     * @return
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;
    }

    /**
     * 获取页面的空闲空间大小
     * @param pg
     * @return
     */
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    /**
     * 将数据插入pg中的offset位置，并将pg的offset设置为较大的offset
     * @param pg
     * @param raw  插入的数据
     * @param offset
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    /**
     * 将数据插入pg中的offset位置，不更新update
     * @param pg
     * @param raw  插入的数据
     * @param offset
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
