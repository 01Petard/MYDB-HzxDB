package top.guoziyang.mydb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * 事务管理器，负责管理数据库中所有的事务
 */
public class TransactionManagerImpl implements TransactionManager {

    // 超级事务，xid为0的事务永远为commited状态
    public static final long SUPER_XID = 0;
    // XID文件头长度，记录了这个 XID 文件管理的事务的个数
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 事务文件的后缀
    static final String XID_SUFFIX = ".xid";
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;
    /**
     * 事务的三种状态
     * 0，active，正在进行，尚未结束
     * 1，committed，已提交
     * 2，aborted，已撤销（回滚）
     */
    // active，正在进行，尚未结束
    private static final byte FIELD_TRAN_ACTIVE = 0;
    // 1，committed，已提交
    private static final byte FIELD_TRAN_COMMITTED = 1;
    // 2，aborted，已撤销（回滚）
    private static final byte FIELD_TRAN_ABORTED = 2;
    // xid的计数
    private long xidCounter;
    private RandomAccessFile file;
    private FileChannel fc;
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     * @param xid
     * @return
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;
    }

    /**
     * 更新xid事务的状态为status
     * @param xid
     * @param status
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将XID加一，并更新XID Header
     */
    private void incrXIDCounter() {
        xidCounter ++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 开始一个事务，并返回XID
     * @return
     */
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 提交XID事务
     * @param xid
     */
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 回滚XID事务
     * @param xid
     */
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 检测XID事务是否处于什么状态
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
