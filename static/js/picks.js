// Async-load analyst picks by industry
document.addEventListener("DOMContentLoaded", function () {
    var select = document.getElementById("industry-select");
    var btn = document.getElementById("load-picks-btn");
    var container = document.getElementById("picks-content");

    if (!select || !btn || !container) return;

    // Enable button when an industry is selected
    select.addEventListener("change", function () {
        btn.disabled = !select.value;
    });

    btn.addEventListener("click", function () {
        var key = select.value;
        if (!key) return;

        btn.disabled = true;
        btn.textContent = "Loading...";

        // Show skeleton
        container.innerHTML =
            '<div class="animate-pulse space-y-3 p-4">' +
            '<div class="h-4 bg-gray-200 rounded w-1/3"></div>' +
            '<div class="h-64 bg-gray-200 rounded"></div>' +
            '</div>';

        fetch("/api/picks/" + encodeURIComponent(key))
            .then(function (resp) {
                if (!resp.ok) throw new Error("Failed to load picks");
                return resp.text();
            })
            .then(function (html) {
                container.innerHTML = html;
                initAddToTracker();
                initPicksSorting();
                initPicksFilters();
                initComparison();
            })
            .catch(function () {
                container.innerHTML =
                    '<p class="text-red-500 italic p-4">Could not load analyst picks. Please try again.</p>';
            })
            .finally(function () {
                btn.disabled = false;
                btn.textContent = "Load Picks";
            });
    });

    // ── Feature 8: Add to Tracker from Picks ─────────────────────────
    function initAddToTracker() {
        var STORAGE_KEY = "finanalyzer_watchlist";
        var buttons = document.querySelectorAll(".add-to-tracker");
        buttons.forEach(function (btn) {
            btn.addEventListener("click", function () {
                var ticker = btn.getAttribute("data-ticker");
                if (!ticker) return;
                var list = [];
                try { list = JSON.parse(localStorage.getItem(STORAGE_KEY)) || []; } catch(e) {}
                if (list.find(function (i) { return i.ticker === ticker; })) {
                    btn.innerHTML = '<span class="text-xs text-gray-400">Tracked</span>';
                    return;
                }
                btn.disabled = true;
                btn.innerHTML = '<span class="text-xs text-gray-400">...</span>';
                fetch("/api/quote/" + ticker)
                    .then(function (resp) {
                        if (!resp.ok) throw new Error("fail");
                        return resp.json();
                    })
                    .then(function (data) {
                        list.push({
                            ticker: ticker,
                            name: data.name || "",
                            price: data.price,
                            change: data.change,
                            changePct: data.changePct,
                            alerts: null
                        });
                        localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
                        btn.innerHTML = '<svg class="w-4 h-4 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>';
                        setTimeout(function () {
                            btn.innerHTML = '<span class="text-xs text-gray-400">Tracked</span>';
                        }, 2000);
                    })
                    .catch(function () {
                        btn.innerHTML = '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"/></svg>';
                        btn.disabled = false;
                    });
            });
        });
    }

    // ── Feature 9: Sortable / Filterable Picks Table ─────────────────
    function initPicksSorting() {
        var sortSelect = document.getElementById("picks-sort");
        if (!sortSelect) return;
        sortSelect.addEventListener("change", function () {
            applyPicksSort(sortSelect.value);
            applyPicksFilters();
        });
    }

    function applyPicksSort(sortValue) {
        var table = document.getElementById("picks-table");
        if (!table) return;
        var tbody = table.querySelector("tbody");
        if (!tbody) return;
        var parts = sortValue.split(":");
        var field = parts[0];
        var dir = parts[1] || "desc";
        var attr = "data-sort-" + field;
        var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr"));
        rows.sort(function (a, b) {
            var aVal = a.getAttribute(attr) || "";
            var bVal = b.getAttribute(attr) || "";
            var aNum = parseFloat(aVal);
            var bNum = parseFloat(bVal);
            var numeric = !isNaN(aNum) && !isNaN(bNum);
            var cmp;
            if (numeric) { cmp = aNum - bNum; } else { cmp = aVal.localeCompare(bVal, undefined, { sensitivity: "base" }); }
            return dir === "desc" ? -cmp : cmp;
        });
        for (var i = 0; i < rows.length; i++) {
            tbody.appendChild(rows[i]);
        }
        restripePicksTable();
    }

    function initPicksFilters() {
        var hideLow = document.getElementById("picks-hide-low");
        var buyOnly = document.getElementById("picks-buy-only");
        if (hideLow) hideLow.addEventListener("change", function () { applyPicksFilters(); });
        if (buyOnly) buyOnly.addEventListener("change", function () { applyPicksFilters(); });
    }

    function applyPicksFilters() {
        var table = document.getElementById("picks-table");
        if (!table) return;
        var tbody = table.querySelector("tbody");
        if (!tbody) return;
        var hideLow = document.getElementById("picks-hide-low");
        var buyOnly = document.getElementById("picks-buy-only");
        var rows = tbody.querySelectorAll("tr");
        rows.forEach(function (row) {
            var hide = false;
            if (hideLow && hideLow.checked && row.getAttribute("data-low-coverage") === "1") hide = true;
            if (buyOnly && buyOnly.checked) {
                var rec = row.getAttribute("data-rec-key") || "";
                if (rec !== "buy" && rec !== "strong_buy") hide = true;
            }
            if (hide) { row.classList.add("hidden"); } else { row.classList.remove("hidden"); }
        });
        restripePicksTable();
    }

    function restripePicksTable() {
        var table = document.getElementById("picks-table");
        if (!table) return;
        var rows = table.querySelectorAll("tbody tr:not(.hidden)");
        for (var i = 0; i < rows.length; i++) {
            rows[i].classList.remove("bg-gray-50", "dark:bg-gray-700/30");
            if (i % 2 === 0) {
                rows[i].classList.add("bg-gray-50");
                rows[i].classList.add("dark:bg-gray-700/30");
            }
        }
    }

    // ── Picks Comparison Mode ────────────────────────────────────────
    function initComparison() {
        var compareBtn = document.getElementById("picks-compare-btn");
        var countEl = document.getElementById("compare-count");
        var panel = document.getElementById("picks-comparison-panel");
        var closeBtn = document.getElementById("close-comparison");
        var contentEl = document.getElementById("comparison-content");
        if (!compareBtn || !panel) return;

        function getChecked() {
            return Array.prototype.slice.call(
                document.querySelectorAll(".picks-compare-cb:checked")
            );
        }

        function updateBtn() {
            var checked = getChecked();
            var n = checked.length;
            if (countEl) countEl.textContent = n;
            if (n >= 2 && n <= 4) {
                compareBtn.classList.remove("hidden");
            } else {
                compareBtn.classList.add("hidden");
            }
        }

        // Limit to 4 selections
        document.addEventListener("change", function(e) {
            if (!e.target.classList.contains("picks-compare-cb")) return;
            var checked = getChecked();
            if (checked.length > 4) {
                e.target.checked = false;
                return;
            }
            updateBtn();
        });

        function fmtMoney(val) {
            val = parseFloat(val);
            if (isNaN(val)) return "N/A";
            if (val >= 1e12) return "$" + (val/1e12).toFixed(2) + "T";
            if (val >= 1e9) return "$" + (val/1e9).toFixed(2) + "B";
            if (val >= 1e6) return "$" + (val/1e6).toFixed(1) + "M";
            return "$" + val.toLocaleString();
        }

        function recBadgeHTML(key) {
            if (!key) return '';
            var k = key.toLowerCase();
            var cls, label = key.toUpperCase().replace(/_/g, ' ');
            if (k === 'buy' || k === 'strong_buy')
                cls = 'bg-green-100 dark:bg-green-900/40 text-green-800 dark:text-green-300';
            else if (k === 'hold')
                cls = 'bg-yellow-100 dark:bg-yellow-900/40 text-yellow-800 dark:text-yellow-300';
            else
                cls = 'bg-red-100 dark:bg-red-900/40 text-red-800 dark:text-red-300';
            return '<span class="inline-block px-2 py-0.5 rounded text-xs font-bold ' + cls + '">' + label + '</span>';
        }

        function renderComparison() {
            var items = getChecked().map(function(cb) {
                return {
                    symbol: cb.dataset.symbol,
                    name: cb.dataset.name,
                    price: parseFloat(cb.dataset.price),
                    target: parseFloat(cb.dataset.target),
                    upside: parseFloat(cb.dataset.upside),
                    rating: cb.dataset.rating,
                    analysts: parseInt(cb.dataset.analysts),
                    mcap: parseFloat(cb.dataset.mcap),
                };
            });
            if (items.length < 2) return;

            // Find best in each metric
            var bestUpside = Math.max.apply(null, items.map(function(i){return i.upside;}));
            var bestAnalysts = Math.max.apply(null, items.map(function(i){return i.analysts;}));
            var bestMcap = Math.max.apply(null, items.map(function(i){return i.mcap;}));

            var cols = items.length;
            var html = '<div class="grid gap-4" style="grid-template-columns: repeat(' + cols + ', minmax(0, 1fr))">';
            items.forEach(function(item) {
                html += '<div class="text-center">';
                html += '<a href="/dashboard/' + item.symbol + '" class="text-brand dark:text-blue-300 font-bold hover:underline">' + item.symbol + '</a>';
                html += '<div class="text-xs text-gray-500 dark:text-gray-400 truncate">' + item.name + '</div>';
                html += '</div>';
            });
            html += '</div>';

            // Metrics rows
            var metrics = [
                {label: "Price", key: "price", fmt: function(v){return "$"+v.toFixed(2);}, best: null},
                {label: "Target", key: "target", fmt: function(v){return "$"+v.toFixed(2);}, best: null},
                {label: "Upside", key: "upside", fmt: function(v){return (v>=0?"+":"")+v.toFixed(1)+"%";}, best: bestUpside},
                {label: "Rating", key: "rating", fmt: function(v){return recBadgeHTML(v);}, best: null, isHtml: true},
                {label: "Analysts", key: "analysts", fmt: function(v){return v;}, best: bestAnalysts},
                {label: "Market Cap", key: "mcap", fmt: function(v){return fmtMoney(v);}, best: bestMcap},
            ];

            html += '<div class="mt-3 space-y-0">';
            metrics.forEach(function(m, mi) {
                var bg = mi % 2 === 0 ? 'bg-gray-50 dark:bg-gray-700/30' : '';
                html += '<div class="grid gap-4 py-2 px-2 rounded ' + bg + '" style="grid-template-columns: repeat(' + cols + ', minmax(0, 1fr))">';
                items.forEach(function(item) {
                    var val = item[m.key];
                    var isBest = m.best !== null && val === m.best;
                    var highlight = isBest ? 'bg-green-50 dark:bg-green-900/20 rounded px-1' : '';
                    html += '<div class="text-center ' + highlight + '">';
                    html += '<div class="text-[10px] text-gray-400 dark:text-gray-500 uppercase">' + m.label + '</div>';
                    html += '<div class="text-sm font-medium text-gray-800 dark:text-gray-200">' + m.fmt(val) + '</div>';
                    html += '</div>';
                });
                html += '</div>';
            });
            html += '</div>';

            contentEl.innerHTML = html;
            panel.classList.remove("hidden");
            panel.scrollIntoView({behavior: "smooth", block: "nearest"});
        }

        compareBtn.addEventListener("click", renderComparison);

        if (closeBtn) {
            closeBtn.addEventListener("click", function() {
                panel.classList.add("hidden");
                document.querySelectorAll(".picks-compare-cb:checked").forEach(function(cb) {
                    cb.checked = false;
                });
                updateBtn();
            });
        }
    }
});
