package com.theastrocreeper.phantomshare;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.content.pm.ServiceInfo;
import android.os.IBinder;
import android.os.Handler;
import android.os.Looper;
import android.os.PowerManager;

/** Keeps the user-started peer session visible to Android while the UI is hidden.
 * The Activity owns the WebView; this service never resurrects a lost session.
 */
public final class ShareConnectionService extends Service {
    private static final String CHANNEL = "phantom-connection";
    private PowerManager.WakeLock wakeLock;
    private String token;
    private SessionWakeLease wakeLease;
    private boolean transferring;

    @Override public void onCreate() {
        super.onCreate();
        NotificationManager notifications = getSystemService(NotificationManager.class);
        notifications.createNotificationChannel(new NotificationChannel(CHANNEL,
                "Device connection", NotificationManager.IMPORTANCE_LOW));
        wakeLock = getSystemService(PowerManager.class).newWakeLock(
                PowerManager.PARTIAL_WAKE_LOCK, "PhantomShare:session");
        wakeLock.setReferenceCounted(false);
        Handler handler = new Handler(Looper.getMainLooper());
        wakeLease = new SessionWakeLease(timeout -> {
            release(); wakeLock.acquire(timeout);
        }, this::release, new SessionWakeLease.Scheduler() {
            public void schedule(Runnable task, long delay) { handler.postDelayed(task, delay); }
            public void cancel(Runnable task) { handler.removeCallbacks(task); }
        });
    }
    @Override public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent == null) { stopSelf(); return START_NOT_STICKY; }
        String next = intent.getStringExtra("token");
        boolean update = intent.hasExtra("transfer");
        if (!update) {
            token = next; transferring = false;
            wakeLease.start();
        } else if (token == null || !token.equals(next)) return START_NOT_STICKY;
        boolean active = update && intent.getBooleanExtra("transfer", false);
        boolean changed = transferring != active;
        transferring = active;
        if (!update || changed) {
            PendingIntent open = PendingIntent.getActivity(this, 0,
                    new Intent(this, MainActivity.class).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP),
                    PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT);
            Notification notification = new Notification.Builder(this, CHANNEL)
                    .setSmallIcon(android.R.drawable.stat_sys_upload)
                    .setContentTitle("Phantom Share")
                    .setContentText(active ? "Transferring files" : "Device session active")
                    .setContentIntent(open).setOngoing(true).setOnlyAlertOnce(true)
                    .setCategory(Notification.CATEGORY_SERVICE).build();
            startForeground(1, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE);
        }
        return START_NOT_STICKY;
    }
    private void release() { if (wakeLock != null && wakeLock.isHeld()) wakeLock.release(); }
    @Override public void onDestroy() {
        if (wakeLease != null) wakeLease.close();
        token = null; super.onDestroy();
    }
    @Override public IBinder onBind(Intent intent) { return null; }
}
