package simulator;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

/**
 * @Author weiyu
 * @Description
 * @Date 2018/10/30 16:22
 */
public class StringSimulator implements Runnable {
    private static final String[] LINES = new String[]{"hello Spark", "hello Storm", "hello Flink"};

    public void run() {
        Random r = new Random();
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(9999);
            System.out.println("******成功开启数据模拟模块，模拟streaming******");
            //开始监听
            Socket socket = serverSocket.accept();
            //创建输出流
            OutputStream os = socket.getOutputStream();
            //包装输出流
            Writer writer = new BufferedWriter(new OutputStreamWriter(os));
            while (true) {
                //随机消息数
                int msgNum = r.nextInt(LINES.length);
                //发送消息
                System.out.println("******发送的消息为"+LINES[msgNum]+"******");
                writer.write(LINES[msgNum] + "\n");
                writer.flush();
                Thread.sleep(2000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        new Thread(new StringSimulator()).start();
    }
}
