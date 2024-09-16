package top.guoziyang.mydb.backend.vm;

import com.google.common.primitives.Bytes;
import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [data]
 * XMIN 是创建该条记录（版本）的事务编号，
 * XMAX 则是删除该条记录（版本）的事务编号。当上层模块通过 VM 删除某个 Entry 时，实际的操作是设置其 XMAX 为某条事务的编号，
 * 由于设置了 XMAX，当后续再次尝试读取该 Entry 时，会因为寻找不到合适的版本而返回 not found 的错误。从而实现了事务间的隔离性。
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    /**
     * VersionEntry引用对象的id
     */
    private long uid;
    /**
     * VersionEntry的引用对象
     */
    private DataItem dataItem;
    private VersionManager vm;

    /**
     * 根据dataItem创建entry
     * @param vm
     * @param uid 索引id
     * @return
     * @throws Exception
     */
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl) vm).dm.read(uid);
        if (di == null) {
            return null;
        }
        Entry ve = new Entry();
        ve.uid = uid;
        ve.dataItem = di;
        ve.vm = vm;
        return ve;
    }

    /**
     * 创建记录时，实例化Entry
     * @param xid
     * @param data
     * @return
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        ((VersionManagerImpl) vm).releaseEntry(this);
    }

    public void remove() {
        dataItem.release();
    }

    /**
     * 获取记录持有的数据
     * @return 以拷贝的形式返回内容
     */
    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取创建记录的事务id
     * @return
     */
    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start + OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取删除记录的事务id
     * @return
     */
    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMAX, sa.start + OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 设置删除记录的事务id
     * @param xid
     */
    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start + OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    /**
     *
     * @return
     */
    public long getUid() {
        return uid;
    }
}
