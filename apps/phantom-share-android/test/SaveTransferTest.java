package com.theastrocreeper.phantomshare;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
public final class SaveTransferTest {
    interface Operation { void run() throws Exception; }
    static void rejects(Operation operation) throws Exception {
        try { operation.run(); throw new AssertionError("Expected failure"); }
        catch (IOException expected) { }
    }
    public static void main(String[] args) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SaveTransfer transfer = new SaveTransfer(output, 3);
        rejects(() -> transfer.append(1, new byte[]{1}));
        transfer.append(0, new byte[]{1,2});
        rejects(() -> transfer.append(0, new byte[]{1}));
        rejects(transfer::finish);
        rejects(() -> transfer.append(1, new byte[]{3,4}));
        transfer.append(1, new byte[]{3}); transfer.finish();
        if (!Arrays.equals(output.toByteArray(), new byte[]{1,2,3})) throw new AssertionError("Bytes changed");
        rejects(() -> transfer.append(2, new byte[]{4}));
        SaveTransfer aborted = new SaveTransfer(new ByteArrayOutputStream(), 10);aborted.abort();aborted.abort();
        rejects(() -> aborted.append(0,new byte[]{1}));rejects(aborted::finish);
        new SaveTransfer(new ByteArrayOutputStream(),0).finish();
        SaveTransfer large = new SaveTransfer(new ByteArrayOutputStream(),100000);
        rejects(() -> large.append(0,new byte[65537]));large.abort();
        System.out.println("SaveTransfer: order, bounds, completeness, exact bytes, empty save and abort passed");
    }
}
