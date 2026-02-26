// Portfolio insight widgets — async loader with staggered fetches.
// Reads widget metadata from a JSON <script> block embedded in portfolio results,
// then fires parallel fetches for phase-1 widgets and a delayed fetch for phase-2.

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
        rows[i].classList.remove("bg-gray-50");
        if (i % 2 === 0) rows[i].classList.add("bg-gray-50");
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

    // Phase 1: sector momentum, news digest, AI commentary — fire in parallel
    var phase1 = [
        { id: "widget-sector-momentum", url: "/api/portfolio/widget/sector-momentum", body: { portfolioSectors: meta.portfolioSectors || {} } },
        { id: "widget-news-digest", url: "/api/portfolio/widget/news-digest", body: { holdings: meta.holdings || [] } },
        { id: "widget-ai-commentary", url: "/api/portfolio/widget/ai-commentary", body: { holdings: meta.holdings || [], bySector: meta.bySector || [], concentration: meta.concentration || [], analystOverview: meta.analystOverview || {} } },
    ];

    phase1.forEach(function (w) {
        fetchWidget(w.id, w.url, w.body);
    });

    // Phase 2: peer valuation — delayed 2s to let phase 1 settle
    setTimeout(function () {
        fetchWidget(
            "widget-peer-valuation",
            "/api/portfolio/widget/peer-valuation",
            { holdings: meta.holdings || [] }
        );
    }, 2000);
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
        })
        .catch(function () {
            el.innerHTML =
                '<p class="text-gray-400 text-sm italic py-2">Could not load this widget. Try refreshing the page.</p>';
        });
}
