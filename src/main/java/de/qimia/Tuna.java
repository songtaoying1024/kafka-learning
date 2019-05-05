package de.qimia;

import java.util.Random;
class Myclass extends Thread{
    public void run(){
        for(int i=0;i<10;i++){
            System.out.println(Thread.currentThread().getId()+ " value "+ i);
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

//Runnable interface contains run() method
public class Tuna{

    public static void main(String[] args) {

        Myclass myclass=new Myclass();
        myclass.start();
        Myclass myclass1=new Myclass();
        myclass1.start();
    }
}
