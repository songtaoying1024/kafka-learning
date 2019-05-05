package de.qimia;
public class Demo {
    private static int count=0;
    public static synchronized void inccount(){
        count++;
    }
    public static void main(String[] args) {
        Thread t1=new Thread(new Runnable() {
            @Override
            public void run() {
                for(int i=0;i<10000;i++){
                    //System.out.println(Thread.currentThread().getId()+ " value "+ i);
                    inccount();
                }

            }
        });
        Thread t2=new Thread(new Runnable(){
            @Override
            public void run() {
                for(int i=0;i<10000;i++){
                    inccount();
                }
            }
        });
        t1.start();
        t2.start();
        try {//java thread can be used to pause the current thread execution until unless the specified thread is dead
            t1.join();
            t2.join();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        System.out.println("value: "+count);
        //System.out.println("value : "+count);
    }
}
