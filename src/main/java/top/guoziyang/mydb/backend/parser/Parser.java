package top.guoziyang.mydb.backend.parser;

import top.guoziyang.mydb.backend.parser.statement.*;
import top.guoziyang.mydb.common.Error;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    public static Object Parse(byte[] statement) throws Exception {
        // 解析sql语句
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();
        Object stat = null;
        Exception statErr = null;
        try {
            switch (token) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                case "help":
                    stat = parseHelp(tokenizer);
                    break;
                default:
                    throw Error.InvalidCommandException;
            }
        } catch (Exception e) {
            statErr = e;
        }


        try {
            // 检查是否还有未解析的token，如果有会报错
            String next = tokenizer.peek();
            if (!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch (Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        if (statErr != null) {
            throw statErr;
        }
        return stat;
    }

    private static Object parseShow(Tokenizer tokenizer) throws Exception {
        if ("".equals(tokenizer.peek())) {
            return new Show();
        } else if ("tables".equals(tokenizer.peek())) {
            tokenizer.pop();
            return new ShowTables();
        }
        throw Error.InvalidCommandException;
    }

    private static Help parseHelp(Tokenizer tokenizer) throws Exception {
        if ("".equals(tokenizer.peek())) {
            return new Help();
        }
        throw Error.InvalidCommandException;
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();

        // 修改的列名
        tokenizer.pop();
        if (!"set".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        // 列修改后的值
        tokenizer.pop();
        update.fieldName = tokenizer.peek();
        tokenizer.pop();
        if (!"=".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        update.value = tokenizer.peek();
        tokenizer.pop();

        // where子句
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);
        return update;
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if (!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        // 表名
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        delete.tableName = tableName;

        // where子句
        tokenizer.pop();
        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if (!"into".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        //表名
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        insert.tableName = tableName;

        // 列名
        tokenizer.pop();
        if (!"values".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        List<String> values = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if (!"".equals(value)) {
                values.add(value);
            } else {
                break;
            }
        }
        insert.values = values.toArray(new String[0]);

        return insert;
    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select select = new Select();

        // 获取查询目标
        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if ("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while (true) {
                String field = tokenizer.peek();
                if (!isName(field)) {
                    throw Error.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if (",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        select.fields = fields.toArray(new String[0]);

        // 校验 “from” 关键字
        if (!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        // 表名
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        select.tableName = tableName;

        // where条件
        tokenizer.pop();
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            select.where = null;
            return select;
        }
        select.where = parseWhere(tokenizer);
        return select;
    }

    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if (!"where".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        // 运算对象1
        tokenizer.pop();
        where.singleExp1 = parseSingleExp(tokenizer);

        // 逻辑运算符
        String logicOp = tokenizer.peek();
        if ("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if (!isLogicOp(logicOp)) {
            throw Error.InvalidCommandException;
        }
        where.logicOp = logicOp;

        // 运算对象2
        tokenizer.pop();
        where.singleExp2 = parseSingleExp(tokenizer);
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return where;
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();

        String field = tokenizer.peek();
        if (!isName(field)) {
            throw Error.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if (!isCmpOp(op)) {
            throw Error.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if (!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        Drop drop = new Drop();

        // 表名
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        drop.tableName = tableName;

        // 命令结束检查
        tokenizer.pop();
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return drop;
    }

    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if (!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        // 表名
        tokenizer.pop();
        Create create = new Create();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        create.tableName = tableName;

        // 字段信息
        List<String> fieldNames = new ArrayList<>();
        List<String> fieldTypes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if ("(".equals(field)) {
                break;
            }

            if (!isName(field)) {
                throw Error.InvalidCommandException;
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if (!isType(fieldType)) {
                throw Error.InvalidCommandException;
            }
            fieldNames.add(field);
            fieldTypes.add(fieldType);
            tokenizer.pop();

            String next = tokenizer.peek();
            if (",".equals(next)) {
                continue;
            } else if ("".equals(next)) {
                throw Error.TableNoIndexException;
            } else if ("(".equals(next)) {
                break;
            } else {
                throw Error.InvalidCommandException;
            }
        }
        create.fieldName = fieldNames.toArray(new String[0]);
        create.fieldType = fieldTypes.toArray(new String[0]);

        // 索引信息
        tokenizer.pop();
        if (!"index".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        List<String> indexes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if (")".equals(field)) {
                break;
            }
            if (!isName(field)) {
                throw Error.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[0]);

        // 建表结束，检查命令是否结束
        tokenizer.pop();
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return create;
    }

    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
                "string".equals(tp));
    }

    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Abort();
    }

    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if (!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Commit();
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        if ("".equals(isolation)) {
            return begin;
        }
        if (!"isolation".equals(isolation)) {
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();
        String level = tokenizer.peek();
        if (!"level".equals(level)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if ("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("committed".equals(tmp2)) {
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else if ("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else {
            throw Error.InvalidCommandException;
        }
    }

    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }
}
