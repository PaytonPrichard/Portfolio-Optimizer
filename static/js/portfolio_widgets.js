// Portfolio insight widgets — async loader with staggered fetches.
// Reads widget metadata from a JSON <script> block embedded in portfolio results,
// then fires parallel fetches for phase-1 widgets and a delayed fetch for phase-2.

// ── Settings helpers ────────────────────────────────────────────────
function _getSettingGrowthRate() {
    var el = document.getElementById("setting-growth-rate");
    if (el) {
        var v = parseFloat(el.value);
        if (!isNaN(v) && v >= 1 && v <= 20) return v;
    }
    return 8;
}

function _getSettingTaxRate() {
    var el = document.getElementById("setting-tax-rate");
    if (el) {
        var v = parseFloat(el.value);
        if (!isNaN(v) && v >= 0 && v <= 50) return v / 100;
    }
    return 0.24;
}

// ── Widget registry for retry support ────────────────────────────────
var _widgetRegistry = {};

function retryWidget(id) {
    var entry = _widgetRegistry[id];
    if (!entry) return;
    var el = document.getElementById(id);
    if (el) {
        el.innerHTML =
            '<div class="animate-pulse"><div class="h-3 bg-gray-100 dark:bg-gray-700 rounded w-4/6 mb-2"></div><div class="h-3 bg-gray-100 dark:bg-gray-700 rounded w-3/6"></div></div>';
    }
    fetchWidget(id, entry.url, entry.body);
}

// ── Table sorting ──────────────────────────────────────────────────────
// Each <select> has data-sortable-table pointing to a <table> id.
// Each <tr> in tbody has data-sort-* attributes for sortable fields.
// The select value is "field:dir" e.g. "symbol:asc" or "value:desc".

function initTableSorting() {
    var selects = document.querySelectorAll("select[data-sortable-table]");
    for (var i = 0; i < selects.length; i++) {
        (function (sel) {
            sel.addEventListener("change", function () {
                sortTable(sel.getAttribute("data-sortable-table"), sel.value);
            });
        })(selects[i]);
    }
}

function sortTable(tableId, sortValue) {
    var table = document.getElementById(tableId);
    if (!table) return;
    var tbody = table.querySelector("tbody");
    if (!tbody) return;

    var parts = sortValue.split(":");
    var field = parts[0];
    var dir = parts[1] || "asc";
    var attr = "data-sort-" + field;

    var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr"));

    rows.sort(function (a, b) {
        var aVal = a.getAttribute(attr) || "";
        var bVal = b.getAttribute(attr) || "";

        // Try numeric comparison first
        var aNum = parseFloat(aVal);
        var bNum = parseFloat(bVal);
        var numeric = !isNaN(aNum) && !isNaN(bNum);

        var cmp;
        if (numeric) {
            cmp = aNum - bNum;
        } else {
            cmp = aVal.localeCompare(bVal, undefined, { sensitivity: "base" });
        }
        return dir === "desc" ? -cmp : cmp;
    });

    // Re-apply zebra striping and re-append rows
    for (var i = 0; i < rows.length; i++) {
        rows[i].classList.remove("bg-gray-50", "dark:bg-gray-700/30");
        if (i % 2 === 0) {
            rows[i].classList.add("bg-gray-50");
            rows[i].classList.add("dark:bg-gray-700/30");
        }
        tbody.appendChild(rows[i]);
    }
}

// ── Widget loading ─────────────────────────────────────────────────────

function loadPortfolioWidgets() {
    var metaEl = document.getElementById("portfolio-widget-meta");
    if (!metaEl) return;

    var meta;
    try {
        meta = JSON.parse(metaEl.textContent);
    } catch (e) {
        return;
    }

    // Phase 1: historical performance, sector momentum, news digest, AI commentary — fire in parallel
    var phase1 = [
        { id: "widget-historical-performance", url: "/api/portfolio/widget/historical-performance", body: { holdings: meta.holdings || [], period: "1mo" } },
        { id: "widget-sector-momentum", url: "/api/portfolio/widget/sector-momentum", body: { portfolioSectors: meta.portfolioSectors || {} } },
        { id: "widget-news-digest", url: "/api/portfolio/widget/news-digest", body: { holdings: meta.holdings || [] } },
        { id: "widget-ai-commentary", url: "/api/portfolio/widget/ai-commentary", body: { holdings: meta.holdings || [], bySector: meta.bySector || [], concentration: meta.concentration || [], analystOverview: meta.analystOverview || {} } },
        { id: "widget-ethical-investing", url: "/api/portfolio/widget/ethical-investing", body: { holdings: meta.holdings || [] } },
    ];

    phase1.forEach(function (w) {
        _widgetRegistry[w.id] = { url: w.url, body: w.body };
        fetchWidget(w.id, w.url, w.body);
    });

    // Phase 2: risk, stress test, factor exposure, fee analysis — delayed 2s
    var phase2 = [
        { id: "widget-peer-valuation", url: "/api/portfolio/widget/peer-valuation", body: { holdings: meta.holdings || [] } },
        { id: "widget-correlation", url: "/api/portfolio/widget/correlation", body: { holdings: meta.holdings || [] } },
        { id: "widget-stress-test", url: "/api/portfolio/widget/stress-test", body: { holdings: meta.holdings || [] } },
        { id: "widget-factor-exposure", url: "/api/portfolio/widget/factor-exposure", body: { holdings: meta.holdings || [] } },
        { id: "widget-fee-analysis", url: "/api/portfolio/widget/fee-analysis", body: { holdings: meta.holdings || [], growthRate: _getSettingGrowthRate() } },
    ];
    setTimeout(function () {
        phase2.forEach(function (w) {
            _widgetRegistry[w.id] = { url: w.url, body: w.body };
            fetchWidget(w.id, w.url, w.body);
        });
    }, 2000);

    // Phase 3: risk dashboard, monte carlo, optimizer, fundamentals — delayed 4s (heavy)
    var phase3 = [
        { id: "widget-risk-dashboard", url: "/api/portfolio/widget/risk-dashboard", body: { holdings: meta.holdings || [] } },
        { id: "widget-monte-carlo", url: "/api/portfolio/widget/monte-carlo", body: { holdings: meta.holdings || [], years: 10 } },
        { id: "widget-optimizer", url: "/api/portfolio/widget/optimizer", body: { holdings: meta.holdings || [], mode: "diversification", clientId: window.__clientId || "" } },
        { id: "widget-suggestions", url: "/api/portfolio/widget/suggestions", body: { holdings: meta.holdings || [] } },
        { id: "widget-fundamentals", url: "/api/portfolio/widget/fundamentals", body: { holdings: meta.holdings || [] } },
    ];
    setTimeout(function () {
        phase3.forEach(function (w) {
            _widgetRegistry[w.id] = { url: w.url, body: w.body };
            fetchWidget(w.id, w.url, w.body);
        });
    }, 4000);

    // Init compound growth (client-side only, no fetch)

    // Settings listeners — tax rate recalculates savings, growth rate reloads fee widget
    var taxRateInput = document.getElementById("setting-tax-rate");
    if (taxRateInput) {
        taxRateInput.addEventListener("change", function () {
            var rate = _getSettingTaxRate();
            document.querySelectorAll("[data-tax-savings]").forEach(function (el) {
                var loss = Math.abs(parseFloat(el.getAttribute("data-tax-savings")));
                if (!isNaN(loss)) {
                    el.textContent = "$" + (loss * rate).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                }
            });
            // Update the disclaimer text
            var disclaimer = document.getElementById("tax-rate-disclaimer");
            if (disclaimer) disclaimer.textContent = "Tax savings estimated at " + Math.round(rate * 100) + "% marginal rate. Consult a tax advisor before making decisions. Wash sale rules may apply.";
        });
    }

    var growthRateInput = document.getElementById("setting-growth-rate");
    if (growthRateInput) {
        growthRateInput.addEventListener("change", function () {
            var entry = _widgetRegistry["widget-fee-analysis"];
            if (entry) {
                entry.body.growthRate = _getSettingGrowthRate();
                fetchWidget("widget-fee-analysis", entry.url, entry.body);
            }
        });
    }

    // Suggestions sub-tab toggle — delegated listener.
    var suggestionsEl = document.getElementById("widget-suggestions");
    if (suggestionsEl) {
        suggestionsEl.addEventListener("click", function (e) {
            var btn = e.target.closest("[data-suggest-tab]");
            if (!btn || btn.disabled) return;
            var tab = btn.getAttribute("data-suggest-tab");
            // Update button styles
            suggestionsEl.querySelectorAll("[data-suggest-tab]").forEach(function (b) {
                if (b.disabled) return;
                if (b === btn) {
                    b.className = "px-3 py-2 border-b-2 border-brand dark:border-blue-400 text-brand dark:text-blue-300";
                } else {
                    b.className = "px-3 py-2 border-b-2 border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200";
                }
            });
            // Show selected content, hide others
            suggestionsEl.querySelectorAll("[data-suggest-content]").forEach(function (c) {
                if (c.getAttribute("data-suggest-content") === tab) {
                    c.removeAttribute("hidden");
                } else {
                    c.setAttribute("hidden", "");
                }
            });
        });

        // Holistic threshold slider — filter rows client-side.
        suggestionsEl.addEventListener("input", function (e) {
            if (e.target.id !== "holistic-min-weight") return;
            var threshold = parseFloat(e.target.value);
            var label = suggestionsEl.querySelector("[data-holistic-threshold]");
            if (label) label.textContent = threshold + "%";
            var list = suggestionsEl.querySelector("[data-holistic-list]");
            if (!list) return;
            var shown = 0;
            var total = 0;
            list.querySelectorAll("details[data-weight-pct]").forEach(function (row) {
                total += 1;
                var w = parseFloat(row.getAttribute("data-weight-pct"));
                if (!isNaN(w) && w >= threshold) {
                    row.removeAttribute("hidden");
                    shown += 1;
                } else {
                    row.setAttribute("hidden", "");
                }
            });
            var counter = suggestionsEl.querySelector("[data-holistic-count]");
            if (counter) counter.textContent = shown + " of " + total + " shown";
            var empty = suggestionsEl.querySelector("[data-holistic-empty]");
            if (empty) {
                if (shown === 0 && total > 0) empty.removeAttribute("hidden");
                else empty.setAttribute("hidden", "");
            }
        });
    }

    // Optimizer mode toggle — delegated listener since the template is async-loaded.
    var optimizerEl = document.getElementById("widget-optimizer");
    if (optimizerEl) {
        optimizerEl.addEventListener("click", function (e) {
            var btn = e.target.closest("[data-opt-mode]");
            if (!btn) return;
            var mode = btn.getAttribute("data-opt-mode");
            var entry = _widgetRegistry["widget-optimizer"];
            if (!entry || entry.body.mode === mode) return;
            entry.body.mode = mode;
            // Show the same spinner/loader used on first render.
            var modeLabel = mode === "return_max" ? "Return-Max" : "Diversification";
            optimizerEl.innerHTML =
                '<div class="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-5">' +
                '<h2 class="text-lg font-semibold text-brand dark:text-blue-300 mb-4">Portfolio Optimizer</h2>' +
                '<div class="flex flex-col items-center justify-center py-8">' +
                '<div class="animate-spin rounded-full h-10 w-10 border-4 border-brand dark:border-blue-400 border-t-transparent mb-4"></div>' +
                '<p class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-1">Recomputing for ' + modeLabel + ' mode</p>' +
                '<p class="text-xs text-gray-500 dark:text-gray-400 text-center max-w-md">Re-running Black-Litterman with updated constraints. This can take 30 to 60 seconds.</p>' +
                "</div></div>";
            fetchWidget("widget-optimizer", entry.url, entry.body);
        });
    }
}

function fetchWidget(containerId, url, body) {
    var el = document.getElementById(containerId);
    if (!el) return;

    fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
    })
        .then(function (resp) {
            return resp.text();
        })
        .then(function (html) {
            el.innerHTML = html;
            // If the server returned an error message, add a retry button
            if (html.indexOf('text-red-500') !== -1) {
                var retryDiv = document.createElement('div');
                retryDiv.className = 'mt-2';
                retryDiv.innerHTML = '<button onclick="retryWidget(\'' + containerId + '\')" class="text-xs font-semibold text-brand dark:text-blue-300 hover:underline px-2 py-1 border border-brand/30 dark:border-blue-400/30 rounded">Retry</button>';
                el.appendChild(retryDiv);
            }
        })
        .catch(function () {
            el.innerHTML =
                '<div class="flex items-center gap-3 py-2">' +
                '<p class="text-gray-400 dark:text-gray-400 text-sm italic">Could not load this widget.</p>' +
                '<button onclick="retryWidget(\'' + containerId + '\')" class="text-xs font-semibold text-brand dark:text-blue-300 hover:underline px-2 py-1 border border-brand/30 dark:border-blue-400/30 rounded">Retry</button>' +
                '</div>';
        });
}

// ── Performance widget period switcher ────────────────────────────────

function fetchPerformanceWidget(period) {
    var metaEl = document.getElementById("portfolio-widget-meta");
    if (!metaEl) return;
    var meta;
    try {
        meta = JSON.parse(metaEl.textContent);
    } catch (e) {
        return;
    }
    fetchWidget(
        "widget-historical-performance",
        "/api/portfolio/widget/historical-performance",
        { holdings: meta.holdings || [], period: period }
    );
}

