package top.guoziyang.mydb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.DataManagerImpl;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.backend.utils.Types;

import java.util.Arrays;

/**
 * 一条记录的接口
 * DataItem 是 DataManager 向上层提供的数据抽象。
 * 上层模块通过地址，向 DataManager 请求到对应的 DataItem，再获取到其中的数据。
 * DataItem的结构：
 * | valid | size | data |
 * valid：数据是否可以被覆盖
 * size：数据长度
 * data：数据
 */
public interface DataItem {

    /**
     * 实例化DataItem
     * @param raw
     * @return
     */
    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的offset处解析处DataItem
    static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }

    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void release();

    void lock();

    void unlock();

    void rLock();

    void rUnLock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();
}
