package com.theastrocreeper.phantomshare;

import android.content.Context;
import android.view.View;
import android.webkit.WebView;

/** The connected-device service owns execution, rather than window focus.
 * Chromium maps a hidden window to hidden web contents and may freeze it.
 * Keep the contents runnable only while a user-started session is alive.
 * Activity focus still controls the camera and other visible-only UI actions.
 */
public final class SessionWebView extends WebView {
    private boolean sessionActive;
    private int windowVisibility = View.VISIBLE;

    public SessionWebView(Context context) { super(context); }

    public void setSessionActive(boolean active) {
        if (sessionActive == active) return;
        sessionActive = active;
        super.onWindowVisibilityChanged(active ? View.VISIBLE : windowVisibility);
        if (active) onResume();
    }

    @Override protected void onWindowVisibilityChanged(int visibility) {
        windowVisibility = visibility;
        super.onWindowVisibilityChanged(sessionActive ? View.VISIBLE : visibility);
    }
}
