// Watchlist store backed by localStorage.
// Tracks per-symbol user actions (watching / declined) from Consider Adding
// suggestions. Keyed by ticker, value is {action, at: ISO}. Persists across
// sessions on the same browser profile. No backend sync — profiles are local.

var MM_WATCHLIST_KEY = "mm-watchlist-actions";

var MM_Watchlist = {
    get: function () {
        try {
            var raw = localStorage.getItem(MM_WATCHLIST_KEY);
            return raw ? JSON.parse(raw) : {};
        } catch (e) {
            return {};
        }
    },
    set: function (symbol, action) {
        if (!symbol) return;
        var store = this.get();
        if (!action) {
            delete store[symbol];
        } else {
            store[symbol] = { action: action, at: new Date().toISOString() };
        }
        try {
            localStorage.setItem(MM_WATCHLIST_KEY, JSON.stringify(store));
        } catch (e) {}
    },
    applyState: function (container) {
        if (!container) container = document;
        var store = this.get();
        container.querySelectorAll("[data-watchlist-symbol]").forEach(function (wrap) {
            var sym = wrap.getAttribute("data-watchlist-symbol");
            var current = (store[sym] && store[sym].action) || null;
            wrap.querySelectorAll(".wl-btn").forEach(function (btn) {
                var action = btn.getAttribute("data-watchlist-action");
                if (action === current) {
                    if (action === "watching") {
                        btn.className = "wl-btn px-1.5 py-0.5 rounded text-[10px] font-bold uppercase transition border border-blue-400 dark:border-blue-500 bg-blue-50 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300";
                        btn.textContent = "Watching";
                    } else {
                        btn.className = "wl-btn px-1.5 py-0.5 rounded text-[10px] font-bold uppercase transition border border-gray-400 dark:border-gray-500 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300";
                        btn.textContent = "Passed";
                    }
                } else {
                    if (action === "watching") {
                        btn.className = "wl-btn px-1.5 py-0.5 rounded text-[10px] font-bold uppercase transition border border-gray-200 dark:border-gray-600 text-gray-500 dark:text-gray-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 hover:text-blue-700 dark:hover:text-blue-300";
                        btn.textContent = "Watch";
                    } else {
                        btn.className = "wl-btn px-1.5 py-0.5 rounded text-[10px] font-bold uppercase transition border border-gray-200 dark:border-gray-600 text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 hover:text-gray-700 dark:hover:text-gray-300";
                        btn.textContent = "Pass";
                    }
                }
            });
        });
    },
    handleClick: function (e) {
        var btn = e.target.closest(".wl-btn");
        if (!btn) return;
        var wrap = btn.closest("[data-watchlist-symbol]");
        if (!wrap) return;
        var sym = wrap.getAttribute("data-watchlist-symbol");
        var action = btn.getAttribute("data-watchlist-action");
        var current = (this.get()[sym] && this.get()[sym].action) || null;
        this.set(sym, current === action ? null : action);
        this.applyState(wrap);
    },
};

document.addEventListener("click", function (e) {
    MM_Watchlist.handleClick(e);
});
