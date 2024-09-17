package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.RandomUtil;

/**
 * 管理第一页，
 * 数据页的第一页用于一些页数用途，
 * 此项目中用于判断上一次数据库是否正常关闭，
 * 特殊页结构：
 * db启动时在100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 */
public class PageOne {
    /**
     * ValidCheck的起始位置
     */
    private static final int OF_VC = 100;
    /**
     * ValidCheck的字符串长度
     */
    private static final int LEN_VC = 8;
    /**
     * 第一次创建表时，在100~107字节处填入一个随机字节
     * @return
     */
    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    /**
     * 启动时，在100~107字节处填入一个随机字节
     * @param raw
     */
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    /**
     * 关闭时，将100~107字节处的字节拷贝到108~115字节
     * @param raw
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }


    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 判断数据页是否正常关闭
     * @param raw
     * @return 数据页是否正常关闭
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}
