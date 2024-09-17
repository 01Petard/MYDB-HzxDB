package top.guoziyang.mydb.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import top.guoziyang.mydb.transport.Encoder;
import top.guoziyang.mydb.transport.Packager;
import top.guoziyang.mydb.transport.Transporter;

public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        // 建立连接
        Socket socket = new Socket("127.0.0.1", 9999);

        // 建立传输层，绑定消息传递类
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        // 建立shelll类，发送、输出结果
        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
