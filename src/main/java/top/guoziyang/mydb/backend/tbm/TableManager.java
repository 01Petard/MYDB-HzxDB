package top.guoziyang.mydb.backend.tbm;

import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.parser.statement.*;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.backend.vm.VersionManager;

public interface TableManager {
    /**
     * 创建一个新的表管理器实例
     * @param path
     * @param vm
     * @param dm
     * @return
     */
    static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    /**
     * 打开一个现有的表管理器实例
     * @param path
     * @param vm
     * @param dm
     * @return
     */
    static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }

    // 开始一个事务
    BeginRes begin(Begin begin);

    // 提交一个事务
    byte[] commit(long xid) throws Exception;

    // 回滚一个事务
    byte[] abort(long xid);

    // 显示事务的状态
    byte[] show(long xid);

    // 创建一个新的表
    byte[] create(long xid, Create create) throws Exception;

    // 插入新的记录
    byte[] insert(long xid, Insert insert) throws Exception;

    // 读取记录
    byte[] read(long xid, Select select) throws Exception;

    // 更新记录
    byte[] update(long xid, Update update) throws Exception;

    // 删除记录
    byte[] delete(long xid, Delete delete) throws Exception;
}
