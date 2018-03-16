package com.weiyu.bigData.demo;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class SimulatorSocket {

    public static void main(String[] args) {
        new Thread(new SimulatorSocketLog()).start();
    }
}

class SimulatorSocketLog implements Runnable {
    //假设一共有200个商品
    private int GOODSID = 200;
    //随机发送消息的条数
    private int MSG_NUM = 30;
    //假设用户浏览该商品的次数
    private int BROWSE_NUM = 5;
    //假设用户浏览商品停留的时间
    private int STAY_TIME = 10;
    //用来体现用户是否收藏，收藏为1，不收藏为0，差评为-1
    int[] COLLECTION = new int[]{-1,0,1};
    //用来模拟用户购买商品的件数，0比较多是为了增加没有买的概率，毕竟不买的还是很多的，很多用户都只是看看
    private int[] BUY_NUM = new int[]{0,1,0,2,0,0,0,1,0};

    public void run() {
        Random r = new Random();
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(9999);
            System.out.println("成功开启数据模拟模块，模仿streaming");
            //开始监听
            Socket socket = serverSocket.accept();
            //创建输出流
            OutputStream os = socket.getOutputStream();
            //包装输出流
            Writer writer = new BufferedWriter(new OutputStreamWriter(os));
            while (true) {
                //随机消息数
                int msgNum = r.nextInt(MSG_NUM) + 1;
                for (int i = 0; i < msgNum; i++) {
                    //消息格式：商品ID::浏览次数::停留时间::是否收藏::购买件数
                    StringBuffer sb = new StringBuffer();
                    sb.append("goodsID-" + (r.nextInt(GOODSID) + 1));
                    sb.append("::");
                    sb.append(r.nextInt(BROWSE_NUM) + 1);
                    sb.append("::");
                    sb.append(r.nextInt(STAY_TIME) + r.nextFloat());
                    sb.append("::");
                    sb.append(COLLECTION[r.nextInt(2)]);
                    sb.append("::");
                    sb.append(BUY_NUM[r.nextInt(9)]);
                    System.out.println(sb.toString());
                    //发送消息
                    writer.write(sb.toString() + "\n");
                }
                writer.flush();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
