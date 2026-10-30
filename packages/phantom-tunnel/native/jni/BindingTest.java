import io.phantom.tunnel.NativeTunnel;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.concurrent.*;
import java.util.regex.*;

public final class BindingTest {
    private static String config(String local,String remote) {
        return "{\"localPeer\":\""+local+"\",\"remotePeer\":\""+remote+"\",\"sessionId\":\"0123456789abcdef0123456789abcdef\",\"bindAddress\":\"127.0.0.1\",\"deadlineMs\":5000}";
    }
    private static String digest(String receipt) {
        Matcher match=Pattern.compile("\\\"sha256\\\":\\\"([0-9a-f]{64})\\\"").matcher(receipt);
        if(!match.find())throw new AssertionError("Missing hash receipt");return match.group(1);
    }
    public static void main(String[] args) throws Exception {
        ExecutorService workers=Executors.newFixedThreadPool(2);
        try {
            try(NativeTunnel a=NativeTunnel.prepare(config("alice","bob"));NativeTunnel b=NativeTunnel.prepare(config("bob","alice"))) {
                String ao=a.offer(),bo=b.offer();
                Future<?> dial=workers.submit(()->{try{a.connect(bo,true);}catch(IOException e){throw new RuntimeException(e);}});
                b.connect(ao,false);dial.get(6,TimeUnit.SECONDS);
                int bytes=4*1024*1024;
                Future<String> sent=workers.submit(()->a.runProbe(bytes,true));
                String received=b.runProbe(bytes,false),source=sent.get(10,TimeUnit.SECONDS);
                byte[] block=new byte[65536];java.util.Arrays.fill(block,(byte)0xa7);
                MessageDigest hash=MessageDigest.getInstance("SHA-256");for(int n=0;n<bytes;n+=block.length)hash.update(block);
                StringBuilder expected=new StringBuilder();for(byte value:hash.digest())expected.append(String.format("%02x",value&255));
                if(!digest(received).equals(expected.toString())||!digest(source).equals(expected.toString()))throw new AssertionError("Probe hash mismatch");
            }
            try(NativeTunnel a=NativeTunnel.prepare(config("alice","bob"));NativeTunnel b=NativeTunnel.prepare(config("bob","alice"))) {
                String remote=b.offer();CountDownLatch started=new CountDownLatch(1);
                Future<Boolean> pending=workers.submit(()->{started.countDown();try{a.connect(remote,true);return false;}catch(IOException e){return true;}});
                started.await();Thread.sleep(50);
                long start=System.nanoTime();a.close();if(!pending.get(2,TimeUnit.SECONDS))throw new AssertionError("Setup was not cancelled");
                if(System.nanoTime()-start>2_000_000_000L)throw new AssertionError("Cancellation stalled");a.close();
            }
            boolean rejected=false;try{NativeTunnel.prepare("{}");}catch(IOException e){rejected=true;}if(!rejected)throw new AssertionError("Invalid config accepted");
            System.out.println("JNI verified: 4 MiB native hash/receipt, cancellation, idempotent close, invalid config.");
        } finally {workers.shutdownNow();}
    }
}
