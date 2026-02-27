// Portfolio insight widgets — async loader with staggered fetches.
// Reads widget metadata from a JSON <script> block embedded in portfolio results,
// then fires parallel fetches for phase-1 widgets and a delayed fetch for phase-2.

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

    // Phase 2: peer valuation + correlation — delayed 2s to let phase 1 settle
    var phase2 = [
        { id: "widget-peer-valuation", url: "/api/portfolio/widget/peer-valuation", body: { holdings: meta.holdings || [] } },
        { id: "widget-correlation", url: "/api/portfolio/widget/correlation", body: { holdings: meta.holdings || [] } },
    ];
    setTimeout(function () {
        phase2.forEach(function (w) {
            _widgetRegistry[w.id] = { url: w.url, body: w.body };
            fetchWidget(w.id, w.url, w.body);
        });
    }, 2000);

    // Init compound growth (client-side only, no fetch)
    initCompoundGrowth();
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
                '<div class="flex items-center gap-3 py-2">' +
                '<p class="text-gray-400 dark:text-gray-500 text-sm italic">Could not load this widget.</p>' +
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

// ── Compound Growth Projection (client-side) ──────────────────────────

function initCompoundGrowth() {
    var widget = document.getElementById("compound-growth-widget");
    if (!widget) return;

    var totalValue = parseFloat(widget.getAttribute("data-total-value"));
    if (!totalValue || totalValue <= 0) return;

    var ageInput = document.getElementById("growth-age-input");
    var targetAgeInput = document.getElementById("growth-target-age");
    var rateInput = document.getElementById("growth-rate-input");
    var output = document.getElementById("growth-output");
    if (!ageInput || !output) return;

    function recalc() {
        var age = parseInt(ageInput.value, 10);
        var targetAge = targetAgeInput ? parseInt(targetAgeInput.value, 10) : 65;
        var ratePct = rateInput ? parseFloat(rateInput.value) : 10;

        if (isNaN(age) || age < 1) {
            output.innerHTML = "";
            return;
        }
        if (isNaN(targetAge) || targetAge < 2) targetAge = 65;
        if (isNaN(ratePct)) ratePct = 10;
        if (ratePct < 0) ratePct = 0;
        if (ratePct > 50) ratePct = 50;

        if (age >= targetAge) {
            output.innerHTML = '<p class="text-sm text-gray-500 dark:text-gray-400 py-4 text-center">Your current age is already at or past the target age of ' + targetAge + '.</p>';
            return;
        }

        var years = targetAge - age;
        var rate = ratePct / 100;
        var values = [totalValue];
        for (var y = 1; y <= years; y++) {
            values.push(values[y - 1] * (1 + rate));
        }
        var projected = values[values.length - 1];
        var multiple = projected / totalValue;

        // Find milestone years (doubles, triples)
        var milestones = [];
        var targets = [
            { label: "2x", mult: 2 },
            { label: "3x", mult: 3 },
            { label: "5x", mult: 5 },
            { label: "10x", mult: 10 }
        ];
        for (var t = 0; t < targets.length; t++) {
            for (var m = 0; m < values.length; m++) {
                if (values[m] >= totalValue * targets[t].mult) {
                    milestones.push({ label: targets[t].label, year: m, age: age + m });
                    break;
                }
            }
        }

        // Format money helper
        function fmt(v) {
            if (v >= 1e9) return "$" + (v / 1e9).toFixed(2) + "B";
            if (v >= 1e6) return "$" + (v / 1e6).toFixed(1) + "M";
            if (v >= 1e3) return "$" + Math.round(v).toLocaleString();
            return "$" + v.toFixed(2);
        }

        // Stat cards
        var rateDisplay = ratePct % 1 === 0 ? ratePct.toFixed(0) : ratePct.toFixed(1);
        var html = '<div class="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">';
        html += '<div class="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3"><div class="text-xs text-gray-500 dark:text-gray-400 uppercase font-semibold mb-1">Years to ' + targetAge + '</div><div class="text-lg font-bold text-[#1F4E79] dark:text-blue-300">' + years + '</div></div>';
        html += '<div class="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3"><div class="text-xs text-gray-500 dark:text-gray-400 uppercase font-semibold mb-1">Projected Value</div><div class="text-lg font-bold text-green-700 dark:text-green-400">' + fmt(projected) + '</div></div>';
        html += '<div class="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3"><div class="text-xs text-gray-500 dark:text-gray-400 uppercase font-semibold mb-1">Growth Multiple</div><div class="text-lg font-bold text-[#1F4E79] dark:text-blue-300">' + multiple.toFixed(1) + 'x</div></div>';
        html += '<div class="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3"><div class="text-xs text-gray-500 dark:text-gray-400 uppercase font-semibold mb-1">Annual Rate</div><div class="text-lg font-bold text-[#1F4E79] dark:text-blue-300">' + rateDisplay + '%</div></div>';
        html += '</div>';

        // Milestone badges
        if (milestones.length > 0) {
            html += '<div class="flex flex-wrap gap-2 mb-4">';
            for (var mi = 0; mi < milestones.length; mi++) {
                var ms = milestones[mi];
                html += '<span class="inline-block bg-[#1F4E79]/10 dark:bg-blue-900/30 text-[#1F4E79] dark:text-blue-300 rounded-full px-3 py-1 text-xs font-semibold">' + ms.label + ' in ' + ms.year + ' yr' + (ms.year !== 1 ? 's' : '') + ' (age ' + ms.age + ')</span>';
            }
            html += '</div>';
        }

        // SVG bar chart
        var maxVal = values[values.length - 1];
        var svgW = 800;
        var svgH = 220;
        var padL = 10;
        var padR = 10;
        var padT = 20;
        var padB = 30;
        var chartW = svgW - padL - padR;
        var chartH = svgH - padT - padB;
        // Show at most 40 bars; if more years, sample
        var step = Math.max(1, Math.ceil(values.length / 40));
        var bars = [];
        for (var bi = 0; bi < values.length; bi += step) {
            bars.push({ year: bi, value: values[bi], age: age + bi });
        }
        // Always include last bar
        if (bars[bars.length - 1].year !== values.length - 1) {
            bars.push({ year: values.length - 1, value: values[values.length - 1], age: targetAge });
        }
        var barW = Math.max(2, (chartW / bars.length) - 2);
        var gap = (chartW - barW * bars.length) / (bars.length > 1 ? bars.length - 1 : 1);

        html += '<div class="mb-2"><svg viewBox="0 0 ' + svgW + ' ' + svgH + '" class="w-full h-auto" preserveAspectRatio="xMidYMid meet">';
        html += '<defs><linearGradient id="growthGrad" x1="0" y1="0" x2="1" y2="0"><stop offset="0%" stop-color="#1F4E79"/><stop offset="100%" stop-color="#22c55e"/></linearGradient></defs>';

        for (var bi2 = 0; bi2 < bars.length; bi2++) {
            var b = bars[bi2];
            var bh = (b.value / maxVal) * chartH;
            var bx = padL + bi2 * (barW + gap);
            var by = padT + chartH - bh;
            var pct = bi2 / (bars.length - 1);
            // Interpolate color from brand to green
            var r = Math.round(31 + (34 - 31) * pct);
            var g = Math.round(78 + (197 - 78) * pct);
            var bl = Math.round(121 + (94 - 121) * pct);
            html += '<rect x="' + bx.toFixed(1) + '" y="' + by.toFixed(1) + '" width="' + barW.toFixed(1) + '" height="' + bh.toFixed(1) + '" rx="1" fill="rgb(' + r + ',' + g + ',' + bl + ')" opacity="0.85"/>';

            // Labels on first and last bars
            if (bi2 === 0 || bi2 === bars.length - 1) {
                var isDk = document.documentElement.classList.contains('dark');
                var labelFill = isDk ? '#d1d5db' : '#374151';
                var axFill = isDk ? '#6b7280' : '#9ca3af';
                html += '<text x="' + (bx + barW / 2).toFixed(1) + '" y="' + (by - 4).toFixed(1) + '" font-size="10" fill="' + labelFill + '" font-family="sans-serif" text-anchor="middle">' + fmt(b.value) + '</text>';
                html += '<text x="' + (bx + barW / 2).toFixed(1) + '" y="' + (svgH - 5).toFixed(1) + '" font-size="10" fill="' + axFill + '" font-family="sans-serif" text-anchor="middle">Age ' + b.age + '</text>';
            }
        }
        html += '</svg></div>';

        output.innerHTML = html;
    }

    ageInput.addEventListener("input", recalc);
    if (targetAgeInput) targetAgeInput.addEventListener("input", recalc);
    if (rateInput) rateInput.addEventListener("input", recalc);
}
