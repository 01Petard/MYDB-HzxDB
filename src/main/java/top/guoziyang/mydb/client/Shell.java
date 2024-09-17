package top.guoziyang.mydb.client;

import java.util.Scanner;

/**
 * 接收命令行输入的命令、执行命令、输出结果
 */
public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    /**
     * 执行命令行的命令、打印命令的结果
     */
    public void run() {
        try (Scanner sc = new Scanner(System.in)) {
            while (true) {
                System.out.print(":> ");
                String statStr = sc.nextLine();
                // 退出指令
                if ("exit".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    // 执行命令行输入的指令
                    byte[] res = client.execute(statStr.getBytes());
                    // 输出结果
                    System.out.println(new String(res));
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } finally {
            client.close();
        }
    }
}
