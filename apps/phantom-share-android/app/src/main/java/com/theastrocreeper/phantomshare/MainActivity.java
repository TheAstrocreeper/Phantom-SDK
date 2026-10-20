package com.theastrocreeper.phantomshare;

import android.Manifest;
import android.annotation.SuppressLint;
import android.app.Activity;
import android.app.AlertDialog;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.net.Uri;
import android.net.Network;
import android.net.ConnectivityManager;
import android.os.Handler;
import android.os.Looper;
import android.os.Bundle;
import android.provider.DocumentsContract;
import android.util.Base64;
import android.view.WindowInsets;
import android.webkit.PermissionRequest;
import android.webkit.ValueCallback;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceRequest;
import android.webkit.WebResourceResponse;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Toast;
import android.widget.FrameLayout;
import android.view.ViewGroup;
import android.window.OnBackInvokedDispatcher;
import androidx.webkit.JavaScriptReplyProxy;
import androidx.webkit.WebViewAssetLoader;
import androidx.webkit.WebViewCompat;
import androidx.webkit.WebViewFeature;
import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;

public final class MainActivity extends Activity {
    private static final String ORIGIN = "https://appassets.androidplatform.net";
    private static final String START = ORIGIN + "/assets/share/index.html";
    private static final int PICK_FILES = 10, SAVE_FILE = 11, CAMERA_PERMISSION = 12, NETWORK_PERMISSION = 13, NOTIFICATION_PERMISSION = 14;
    private static final String LOCAL_NETWORK = "android.permission.ACCESS_LOCAL_NETWORK";
    private SessionWebView web;
    private boolean webPaused;
    private ValueCallback<Uri[]> filePicker;
    private PermissionRequest camera;
    private JavaScriptReplyProxy networkReply;
    private String networkRequestId;
    private boolean savePickerOpen;
    private String sessionToken;
    private boolean notificationAsked;
    private ConnectivityManager connectivity;
    private ConnectivityManager.NetworkCallback networkCallback;
    private Network defaultNetwork;
    private final NetworkSessionGuard networkGuard = new NetworkSessionGuard();
    private JSONObject pendingSave;
    private JavaScriptReplyProxy pendingReply;
    // URI, stream, token and generation are only accessed by the I/O executor.
    private SaveTransfer transfer;
    private Uri destination;
    private String saveToken;
    private volatile boolean destroyed;
    private final ThreadPoolExecutor io = new ThreadPoolExecutor(1, 1, 0,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(16));

    private static boolean local(Uri uri) {
        return uri != null && "https".equals(uri.getScheme())
                && "appassets.androidplatform.net".equals(uri.getHost())
                && (uri.getPort() == -1 || uri.getPort() == 443);
    }
    // The bundled Share UI and SDK require JavaScript; navigation stays in local assets.
    @SuppressLint("SetJavaScriptEnabled")
    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        getWindow().setDecorFitsSystemWindows(false);
        FrameLayout content = new FrameLayout(this);
        content.setBackgroundColor(Color.rgb(11, 16, 22));
        web = new SessionWebView(this);
        web.setRendererPriorityPolicy(WebView.RENDERER_PRIORITY_IMPORTANT, false);
        web.setBackgroundColor(Color.rgb(11, 16, 22));
        content.addView(web, new FrameLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT));
        // Inset the parent so WebView receives a genuinely smaller viewport.
        // WebView's own padding does not reliably constrain HTML or hit targets.
        content.setOnApplyWindowInsetsListener((view, insets) -> {
            android.graphics.Insets safe = insets.getInsets(WindowInsets.Type.systemBars()
                    | WindowInsets.Type.displayCutout() | WindowInsets.Type.ime());
            view.setPadding(safe.left, safe.top, safe.right, safe.bottom);
            return WindowInsets.CONSUMED;
        });
        setContentView(content);
        // Request after attachment as well as on later rotations/keyboard changes.
        content.post(content::requestApplyInsets);
        WebSettings settings = web.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setAllowFileAccess(false);
        settings.setAllowContentAccess(true); // Only the native chooser provides file URIs.
        settings.setMixedContentMode(WebSettings.MIXED_CONTENT_NEVER_ALLOW);
        settings.setJavaScriptCanOpenWindowsAutomatically(false);
        settings.setSupportMultipleWindows(false);
        WebView.setWebContentsDebuggingEnabled(BuildConfig.DEBUG);
        WebViewAssetLoader assets = new WebViewAssetLoader.Builder()
                .addPathHandler("/assets/", new WebViewAssetLoader.AssetsPathHandler(this)).build();
        web.setWebViewClient(new WebViewClient() {
            @Override public WebResourceResponse shouldInterceptRequest(WebView view, WebResourceRequest request) {
                WebResourceResponse resource = assets.shouldInterceptRequest(request.getUrl());
                // Never let a missing local asset fall through to the internet.
                if (resource == null && local(request.getUrl())) return new WebResourceResponse(
                        "text/plain", "UTF-8", 404, "Not Found", Collections.emptyMap(),
                        new ByteArrayInputStream(new byte[0]));
                return resource;
            }
            @Override public boolean onRenderProcessGone(WebView view, android.webkit.RenderProcessGoneDetail detail) {
                sessionToken = null;
                stopService(new Intent(MainActivity.this, ShareConnectionService.class));
                toast("Android stopped the session. Reopen Phantom Share to reconnect.");
                finish();
                return true;
            }
            @Override public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                Uri uri = request.getUrl();
                if (local(uri) && uri.getPath() != null && uri.getPath().equals("/assets/share/index.html")) return false;
                if (request.isForMainFrame() && "https".equals(uri.getScheme())) {
                    try { startActivity(new Intent(Intent.ACTION_VIEW, uri).addCategory(Intent.CATEGORY_BROWSABLE)); }
                    catch (RuntimeException ignored) { toast("No browser available"); }
                }
                return true;
            }
        });
        web.setWebChromeClient(new WebChromeClient() {
            @Override public boolean onShowFileChooser(WebView view, ValueCallback<Uri[]> callback, FileChooserParams params) {
                if (filePicker != null) { callback.onReceiveValue(null); return true; }
                filePicker = callback;
                Intent pick = new Intent(Intent.ACTION_OPEN_DOCUMENT).addCategory(Intent.CATEGORY_OPENABLE)
                        .setType("*/*").putExtra(Intent.EXTRA_ALLOW_MULTIPLE, params.getMode() == FileChooserParams.MODE_OPEN_MULTIPLE);
                try { startActivityForResult(pick, PICK_FILES); }
                catch (RuntimeException error) { filePicker = null; callback.onReceiveValue(null); toast("File picker unavailable"); }
                return true;
            }
            @Override public void onPermissionRequest(PermissionRequest request) {
                if (!local(request.getOrigin()) || request.getResources().length != 1
                        || !PermissionRequest.RESOURCE_VIDEO_CAPTURE.equals(request.getResources()[0])) {
                    request.deny(); return;
                }
                if (camera != null) camera.deny();
                camera = request;
                if (checkSelfPermission(Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) grantCamera();
                else requestPermissions(new String[]{Manifest.permission.CAMERA}, CAMERA_PERMISSION);
            }
            @Override public void onPermissionRequestCanceled(PermissionRequest request) {
                if (camera == request) camera = null;
            }
        });
        if (!WebViewFeature.isFeatureSupported(WebViewFeature.WEB_MESSAGE_LISTENER)) {
            new AlertDialog.Builder(this).setMessage("Update Android System WebView to use Phantom Share.")
                    .setPositiveButton("Close", (d, w) -> finish()).setCancelable(false).show();
            return;
        }
        if (WebViewFeature.isFeatureSupported(WebViewFeature.WEB_MESSAGE_LISTENER)) WebViewCompat.addWebMessageListener(web, "PhantomNative", Collections.singleton(ORIGIN),
                (view, message, origin, mainFrame, reply) -> {
                    if (destroyed || !mainFrame || !local(origin)) return;
                    String text = message.getData();
                    if (text == null || text.length() > 90000) return;
                    try { handle(new JSONObject(text), reply); }
                    catch (Exception error) { toast("Invalid app request"); }
                });
        getOnBackInvokedDispatcher().registerOnBackInvokedCallback(
                OnBackInvokedDispatcher.PRIORITY_DEFAULT, this::confirmExit);
        connectivity = getSystemService(ConnectivityManager.class);
        defaultNetwork = connectivity.getActiveNetwork();
        networkCallback = new ConnectivityManager.NetworkCallback() {
            @Override public void onAvailable(Network network) {
                defaultNetwork = network;
                networkChanged(network);
            }
            @Override public void onLost(Network network) {
                if (!network.equals(defaultNetwork)) return;
                defaultNetwork = null;
                networkChanged(null);
            }
        };
        connectivity.registerDefaultNetworkCallback(networkCallback, new Handler(Looper.getMainLooper()));
        web.loadUrl(START);
    }
    private static String routeTag(Network network) {
        return network == null ? null : Long.toString(network.getNetworkHandle());
    }
    private void networkChanged(Network network) {
        String owner = networkGuard.changed(routeTag(network));
        if (owner == null || !owner.equals(sessionToken) || destroyed) return;
        sessionToken = null;
        stopService(new Intent(this, ShareConnectionService.class));
        try {
            JSONObject detail = new JSONObject().put("token", owner)
                    .put("reason", network == null ? "network_lost" : "network_changed");
            web.evaluateJavascript("window.dispatchEvent(new CustomEvent('phantom-network-change',{detail:"
                    + detail + "}))", value -> {
                        if (!destroyed && sessionToken == null) web.setSessionActive(false);
                    });
        } catch (Exception ignored) { web.setSessionActive(false); }
    }
    private void grantCamera() {
        PermissionRequest request = camera; camera = null;
        if (request != null && !destroyed) request.grant(new String[]{PermissionRequest.RESOURCE_VIDEO_CAPTURE});
    }
    @Override public void onRequestPermissionsResult(int code, String[] permissions, int[] results) {
        super.onRequestPermissionsResult(code, permissions, results);
        if (code == NETWORK_PERMISSION) {
            JavaScriptReplyProxy reply = networkReply; String id = networkRequestId;
            networkReply = null; networkRequestId = null;
            if (reply != null) respond(reply, id, true,
                    results.length > 0 && results[0] == PackageManager.PERMISSION_GRANTED ? null
                    : "Allow nearby devices in Android app permissions to connect over Wi-Fi.");
            return;
        }
        if (code != CAMERA_PERMISSION) return;
        if (results.length > 0 && results[0] == PackageManager.PERMISSION_GRANTED) grantCamera();
        else if (camera != null) { camera.deny(); camera = null; }
    }
    private void handle(JSONObject request, JavaScriptReplyProxy reply) throws Exception {
        String id = request.getString("id"), action = request.getString("action");
        if (id.length() > 80) return;
        if (action.equals("network")) {
            if (android.os.Build.VERSION.SDK_INT < 37 || checkSelfPermission(LOCAL_NETWORK) == PackageManager.PERMISSION_GRANTED) {
                respond(reply, id, true, null);
            } else if (networkReply != null) respond(reply, id, null, "Network permission request already open");
            else {
                networkReply = reply; networkRequestId = id;
                requestPermissions(new String[]{LOCAL_NETWORK}, NETWORK_PERMISSION);
            }
            return;
        }
        if (action.equals("startSession")) {
            String token = request.getString("token");
            if (!token.matches("[A-Za-z0-9-]{1,80}")) { respond(reply, id, null, "Invalid session"); return; }
            try {
                startForegroundService(new Intent(this, ShareConnectionService.class)
                        .putExtra("token", token));
                sessionToken = token;
                defaultNetwork = connectivity.getActiveNetwork();
                networkGuard.begin(token, routeTag(defaultNetwork));
                web.setSessionActive(true); webPaused = false;
                if (!notificationAsked && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
                    notificationAsked = true;
                    requestPermissions(new String[]{Manifest.permission.POST_NOTIFICATIONS}, NOTIFICATION_PERMISSION);
                }
                respond(reply, id, true, null);
            } catch (RuntimeException error) {
                respond(reply, id, null, "Return to Phantom Share to start the connection");
            }
            return;
        }
        if (action.equals("transfer") || action.equals("endSession")) {
            if (request.optString("token").equals(sessionToken)) {
                if (action.equals("endSession")) {
                    networkGuard.end(sessionToken);
                    sessionToken = null;
                    web.setSessionActive(false);
                    stopService(new Intent(this, ShareConnectionService.class));
                } else startService(new Intent(this, ShareConnectionService.class)
                        .putExtra("token", sessionToken).putExtra("transfer", request.optBoolean("active")));
            }
            respond(reply, id, true, null);
            return;
        }
        if (action.equals("begin")) {
            if (savePickerOpen) { respond(reply, id, null, "Another save is open"); return; }
            long size = request.getLong("size");
            if (size < 0 || size > 9007199254740991L) { respond(reply, id, null, "Invalid file size"); return; }
            // Reject another save while its stream is open, on the same serial queue.
            submit(() -> {
                if (transfer != null) { respond(reply, id, null, "Another save is active"); return; }
                runOnUiThread(() -> {
                    if (destroyed) return;
                    if (savePickerOpen) { respond(reply, id, null, "Another save is open"); return; }
                    pendingSave = request; pendingReply = reply; savePickerOpen = true;
                    String name = request.optString("name", "download").replaceAll("[\\\\/\\p{Cntrl}]", "_");
                    if (name.length() > 200) name = name.substring(0, 200);
                    String mime = request.optString("type", "application/octet-stream");
                    if (!mime.matches("[A-Za-z0-9.+-]+/[A-Za-z0-9.+-]+")) mime = "application/octet-stream";
                    try { startActivityForResult(new Intent(Intent.ACTION_CREATE_DOCUMENT)
                            .addCategory(Intent.CATEGORY_OPENABLE).setType(mime)
                            .putExtra(Intent.EXTRA_TITLE, name.isEmpty() ? "download" : name), SAVE_FILE); }
                    catch (RuntimeException error) { pendingSave = null; pendingReply = null; savePickerOpen = false; respond(reply, id, null, "Save picker unavailable"); }
                });
            }, reply, id);
            return;
        }
        if (action.equals("cancel")) {
            if (pendingSave != null) {
                respond(pendingReply, pendingSave.optString("id"), null, "Save cancelled");
                pendingSave = null; pendingReply = null;
            }
            submit(() -> { discard(); respond(reply, id, true, null); }, reply, id); return;
        }
        submit(() -> {
            try {
                if (transfer == null || !request.optString("token").equals(saveToken)) throw new Exception("Save is no longer active");
                if (action.equals("chunk")) transfer.append(request.getInt("index"), Base64.decode(request.getString("data"), Base64.NO_WRAP));
                else if (action.equals("finish")) { transfer.finish(); transfer = null; destination = null; saveToken = null; }
                else throw new Exception("Unknown save request");
                respond(reply, id, true, null);
            } catch (Exception error) { discard(); respond(reply, id, null, "Could not save file: " + error.getMessage()); }
        }, reply, id);
    }
    private void submit(Runnable work, JavaScriptReplyProxy reply, String id) {
        try { io.execute(work); } catch (RuntimeException error) { respond(reply, id, null, "Save queue unavailable"); }
    }
    private void discard() {
        if (transfer != null) transfer.abort();
        transfer = null; saveToken = null;
        if (destination != null) {
            try { DocumentsContract.deleteDocument(getContentResolver(), destination); } catch (Exception ignored) { }
        }
        destination = null;
    }
    private void respond(JavaScriptReplyProxy reply, String id, Object result, String error) {
        runOnUiThread(() -> {
            if (destroyed) return;
            try {
                JSONObject response = new JSONObject().put("id", id).put("ok", error == null);
                if (error == null) response.put("result", result == null ? JSONObject.NULL : result);
                else response.put("error", error);
                if (WebViewFeature.isFeatureSupported(WebViewFeature.WEB_MESSAGE_LISTENER))
                    reply.postMessage(response.toString());
            } catch (Exception ignored) { }
        });
    }
    @Override protected void onActivityResult(int code, int result, Intent data) {
        super.onActivityResult(code, result, data);
        if (code == PICK_FILES && filePicker != null) {
            ValueCallback<Uri[]> callback = filePicker; filePicker = null;
            ArrayList<Uri> picked = new ArrayList<>();
            if (result == RESULT_OK && data != null) {
                if (data.getClipData() != null) for (int i = 0; i < Math.min(128, data.getClipData().getItemCount()); i++) {
                    Uri uri = data.getClipData().getItemAt(i).getUri(); if ("content".equals(uri.getScheme())) picked.add(uri);
                }
                else if (data.getData() != null && "content".equals(data.getData().getScheme())) picked.add(data.getData());
            }
            callback.onReceiveValue(picked.isEmpty() ? null : picked.toArray(new Uri[0]));
        }
        if (code == SAVE_FILE) {
            savePickerOpen = false;
            JSONObject request = pendingSave; JavaScriptReplyProxy reply = pendingReply;
            pendingSave = null; pendingReply = null;
            Uri uri = result == RESULT_OK && data != null ? data.getData() : null;
            if (request == null) {
                if (uri != null) submit(() -> { try { DocumentsContract.deleteDocument(getContentResolver(), uri); } catch (Exception ignored) { } }, reply, "");
                return;
            }
            String id = request.optString("id");
            if (uri == null) { respond(reply, id, null, null); return; }
            submit(() -> {
                try {
                    if (!"content".equals(uri.getScheme())) throw new Exception("Invalid destination");
                    destination = uri;
                    OutputStream stream = getContentResolver().openOutputStream(uri, "w");
                    transfer = new SaveTransfer(stream, request.getLong("size"));
                    saveToken = UUID.randomUUID().toString();
                    respond(reply, id, new JSONObject().put("token", saveToken), null);
                } catch (Exception error) { discard(); respond(reply, id, null, "Could not open destination"); }
            }, reply, id);
        }
    }
    private void confirmExit() {
        new AlertDialog.Builder(this).setTitle("Close Phantom Share?")
                .setMessage("Closing ends this connection and any active transfers.")
                .setNegativeButton("Stay", null).setPositiveButton("Close", (d, w) -> finish()).show();
    }
    private void toast(String text) { Toast.makeText(this, text, Toast.LENGTH_LONG).show(); }
    @Override protected void onPause() {
        if (web != null) {
            // A permission sheet is part of the current scan/connect action.
            // Do not invalidate the pending QR scan just because that sheet opens.
            if (camera == null && networkReply == null)
                web.evaluateJavascript("window.dispatchEvent(new Event('phantom-background'))", null);
            if (sessionToken == null) { web.onPause(); webPaused = true; }
        }
        super.onPause();
    }
    @Override protected void onResume() {
        super.onResume();
        if (web != null) {
            if (webPaused) { web.onResume(); webPaused = false; }
            web.evaluateJavascript("window.dispatchEvent(new Event('phantom-foreground'))", null);
        }
    }
    @Override protected void onDestroy() {
        destroyed = true;
        networkGuard.end(sessionToken);
        if (connectivity != null && networkCallback != null) {
            try { connectivity.unregisterNetworkCallback(networkCallback); } catch (RuntimeException ignored) { }
            networkCallback = null;
        }
        sessionToken = null;
        stopService(new Intent(this, ShareConnectionService.class));
        networkReply = null; networkRequestId = null;
        if (filePicker != null) { filePicker.onReceiveValue(null); filePicker = null; }
        if (camera != null) { camera.deny(); camera = null; }
        pendingSave = null; pendingReply = null;
        // Queue cleanup after in-flight writes, then stop accepting work.
        io.getQueue().clear();
        try { io.execute(this::discard); } catch (RuntimeException ignored) { }
        io.shutdown();
        if (web != null) { web.setSessionActive(false); web.stopLoading(); web.destroy(); web = null; }
        super.onDestroy();
    }
}
