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
});
