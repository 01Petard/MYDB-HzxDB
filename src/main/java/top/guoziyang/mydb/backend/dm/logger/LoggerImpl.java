package top.guoziyang.mydb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 实现了日志文件
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 是后续所有日志计算的校验和，用于校验后续所有日志是否损坏
 * Log1 ~ LogN 是常规的日志数据
 * BadTail 是在数据库崩溃时，没有来得及写完的日志数据，BadTail 不一定存在
 * <p/>
 * XChecksum 用于计算后续所有日志的Checksum，校验日志文件是否损坏
 * <p/>
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size：标识Data长度
 * Checksum：校验当前日志文件是否损坏
 * Data：日志数据
 */
public class LoggerImpl implements Logger {
    /**
     * 日志文件后缀
     */
    public static final String LOG_SUFFIX = ".log";
    /**
     * 计算日志校验和一个的种子，如果修改，可能无法读取历史日志和日志文件
     */
    private static final int SEED = 13331;
    /**
     * 日志大小的偏移量
     */
    private static final int OF_SIZE = 0;
    /**
     * 日志校验和的偏移量
     */
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    /**
     * 日志数据的偏移量
     */
    private static final int OF_DATA = OF_CHECKSUM + 4;
    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;
    /**
     * 日志文件读到的位置偏移
     */
    private long position;
    /**
     * 日志文件的大小
     */
    private long fileSize;
    /**
     * 所有日志计算的校验和
     */
    private int xChecksum;

    public LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    public void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    /**
     * 检查并移除bad tail，确保日志文件的一致性
     */
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if (xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        // 去掉 BadTail日志数据，保证日志文件的一致性
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    /**
     * 截断文件到正常日志的末尾
     * @param x
     * @throws Exception
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 单条日志的校验和
     * @param xCheck
     * @param log
     * @return
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 向日志文件写入日志
     * @param data
     */
    @Override
    public void log(byte[] data) {
        // 将数据包裹成日志格式，得到二进制格式的日志
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        byte[] log = Bytes.concat(size, checksum, data);
        // 将日志写入日志文件
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    /**
     * 更新日志文件的校验和
     * @param log
     */
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }



    /**
     * 获取下一个日志
     * Logger 被实现成迭代器模式，便于读取
     * @return
     */
    private byte[] internNext() {
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if (position + size + OF_DATA > fileSize) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    /**
     * 获取下一条日志
     * @return
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 指针回复到校验和的位置
     * @return
     */
    @Override
    public void rewind() {
        position = 4;
    }

    /**
     * 关闭日志的读写操作
     * @return
     */
    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
