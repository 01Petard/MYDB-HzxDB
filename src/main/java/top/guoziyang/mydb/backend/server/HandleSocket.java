package top.guoziyang.mydb.backend.server;


import top.guoziyang.mydb.backend.tbm.TableManager;
import top.guoziyang.mydb.transport.Encoder;
import top.guoziyang.mydb.transport.Package;
import top.guoziyang.mydb.transport.Packager;
import top.guoziyang.mydb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class HandleSocket implements Runnable {
    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress() + ":" + address.getPort());

        Packager packager = getPackager();
        if (packager == null) return;

        // 开始进入数据库，执行sql操作
        execSQL(packager);
    }

    /**
     * 构造一个Packager，用于接收返回处理的结果
     * @return
     */
    private Packager getPackager() {
        Packager packager;
        try {
            packager = new Packager(new Transporter(socket), new Encoder());
        } catch (IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return null;
        }
        return packager;
    }

    /**
     * 构造Executor，执行指令。
     * 执行结果由Packager对象发送出去
     * @param packager 发送执行结果的Packager对象
     */
    private void execSQL(Packager packager) {
        Executor exe = new Executor(tbm);
        while (true) {
            /*
            Transporter从缓冲区读入指令，发送给Packager, Packager转成二进制数组给Package
             */
            Package pkg;
            try {
                // 接收指令
                pkg = packager.receive();
            } catch (Exception e) {
                break;
            }
            // 将指令转成字节数组
            byte[] sql = pkg.getData();

            /*
              执行sql，接收执行结果，返回给Packager发给Transporter，Transporter写入缓冲区
             */
            // 执行结果
            byte[] result = null;
            // 执行失败的异常结果
            Exception e = null;
            try {
                // 通过Executor执行sql，获得执行结果
                result = exe.execute(sql);
            } catch (Exception execExcepetion) {
                e = execExcepetion;
                e.printStackTrace();
            } finally {
                /*
                将执行结果发送给Packager，Packager发送给Transporter，Transporter写入缓冲区
                */
                pkg = new Package(result, e);
            }

            // 发送执行结果
            try {
                // sql执行的结果
                packager.send(pkg);
            } catch (Exception transportException) {
                transportException.printStackTrace();
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}