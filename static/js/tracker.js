// Stock Tracker — localStorage CRUD, quote fetching, alert checks, inline editing
(function () {
    var STORAGE_KEY = "finanalyzer_watchlist";

    function getWatchlist() {
        try {
            return JSON.parse(localStorage.getItem(STORAGE_KEY)) || [];
        } catch (e) {
            return [];
        }
    }

    function saveWatchlist(list) {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
    }

    function esc(str) {
        var div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function renderWatchlist() {
        var list = getWatchlist();
        var tbody = document.getElementById("watchlist-body");
        var emptyMsg = document.getElementById("empty-msg");

        if (!list.length) {
            tbody.innerHTML = "";
            emptyMsg.classList.remove("hidden");
            return;
        }

        emptyMsg.classList.add("hidden");
        var html = "";
        list.forEach(function (item) {
            var priceStr = item.price != null ? "$" + item.price.toFixed(2) : "--";
            var changeStr = "--";
            var changeClass = "text-gray-500";
            if (item.change != null) {
                var sign = item.change >= 0 ? "+" : "";
                changeStr = sign + item.change.toFixed(2) + " (" + sign + (item.changePct || 0).toFixed(2) + "%)";
                changeClass = item.change >= 0 ? "text-green-700 font-semibold" : "text-red-600 font-semibold";
            }

            // Alert check
            var alertTriggered = false;
            var alertBorder = "";
            if (item.price != null) {
                if (item.alerts && item.alerts.above != null && item.price >= item.alerts.above) {
                    alertTriggered = true;
                }
                if (item.alerts && item.alerts.below != null && item.price <= item.alerts.below) {
                    alertTriggered = true;
                }
            }
            if (alertTriggered) {
                alertBorder = "border-l-4 border-yellow-500 bg-yellow-50 animate-pulse";
            }

            var alertText = "";
            if (item.alerts) {
                var parts = [];
                if (item.alerts.above != null) parts.push("&uarr;$" + item.alerts.above);
                if (item.alerts.below != null) parts.push("&darr;$" + item.alerts.below);
                alertText = parts.join(" ") || "--";
            } else {
                alertText = "--";
            }

            html += '<tr class="border-b hover:bg-gray-50 ' + alertBorder + '">';
            html += '<td class="px-4 py-3 font-bold text-brand"><a href="/dashboard/' + esc(item.ticker) + '" class="hover:underline">' + esc(item.ticker) + '</a></td>';
            html += '<td class="px-4 py-3 text-gray-700">' + esc(item.name || "--") + '</td>';
            html += '<td class="px-4 py-3 text-right font-mono">' + priceStr + '</td>';
            html += '<td class="px-4 py-3 text-right ' + changeClass + '">' + changeStr + '</td>';
            html += '<td class="px-4 py-3 text-center text-xs text-gray-500">' + alertText + '</td>';
            html += '<td class="px-4 py-3 text-center space-x-2">';
            html += '<button class="text-brand hover:text-brand-dark text-xs font-semibold" data-edit="' + esc(item.ticker) + '">Edit</button>';
            html += '<button class="text-red-500 hover:text-red-700 text-xs font-semibold" data-remove="' + esc(item.ticker) + '">Remove</button>';
            html += '</td>';
            html += '</tr>';
        });
        tbody.innerHTML = html;

        // Bind remove buttons
        tbody.querySelectorAll("[data-remove]").forEach(function (btn) {
            btn.addEventListener("click", function () {
                removeTicker(btn.dataset.remove);
            });
        });

        // Bind edit buttons
        tbody.querySelectorAll("[data-edit]").forEach(function (btn) {
            btn.addEventListener("click", function () {
                editAlerts(btn.dataset.edit);
            });
        });
    }

    function removeTicker(ticker) {
        var list = getWatchlist().filter(function (item) {
            return item.ticker !== ticker;
        });
        saveWatchlist(list);
        renderWatchlist();
    }

    function editAlerts(ticker) {
        var list = getWatchlist();
        var item = list.find(function (i) { return i.ticker === ticker; });
        if (!item) return;

        var currentAbove = (item.alerts && item.alerts.above != null) ? item.alerts.above : "";
        var currentBelow = (item.alerts && item.alerts.below != null) ? item.alerts.below : "";

        var above = prompt("Alert when " + ticker + " price goes ABOVE $ (leave empty to clear):", currentAbove);
        if (above === null) return; // cancelled

        var below = prompt("Alert when " + ticker + " price goes BELOW $ (leave empty to clear):", currentBelow);
        if (below === null) return; // cancelled

        var alerts = {};
        if (above.trim() !== "") alerts.above = parseFloat(above);
        if (below.trim() !== "") alerts.below = parseFloat(below);

        item.alerts = (alerts.above != null || alerts.below != null) ? alerts : null;
        saveWatchlist(list);
        renderWatchlist();
    }

    function showRefreshError(message) {
        var el = document.getElementById("refresh-error");
        if (el) {
            el.textContent = message;
            el.classList.remove("hidden");
            setTimeout(function() { el.classList.add("hidden"); }, 5000);
        }
    }

    function refreshAll() {
        var list = getWatchlist();
        if (!list.length) return;

        var tickers = list.map(function (item) { return item.ticker; });

        var btn = document.getElementById("refresh-btn");
        btn.disabled = true;
        btn.textContent = "Refreshing...";

        fetch("/api/quotes", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ tickers: tickers }),
        })
        .then(function (resp) {
            if (!resp.ok) throw new Error("Server error");
            return resp.json();
        })
        .then(function (results) {
            var list = getWatchlist();
            var failedCount = 0;
            results.forEach(function (r) {
                var item = list.find(function (i) { return i.ticker === r.symbol; });
                if (item && !r.error) {
                    item.name = r.name || item.name;
                    item.price = r.price;
                    item.change = r.change;
                    item.changePct = r.changePct;
                } else if (r.error) {
                    failedCount++;
                }
            });
            saveWatchlist(list);
            renderWatchlist();
            document.getElementById("last-updated").textContent = "Updated: " + new Date().toLocaleTimeString();
            if (failedCount > 0) {
                showRefreshError(failedCount + " ticker(s) failed to update.");
            }
        })
        .catch(function () {
            showRefreshError("Refresh failed. Check your connection and try again.");
        })
        .finally(function () {
            btn.disabled = false;
            btn.textContent = "Refresh All";
        });
    }

    // Add form
    document.getElementById("add-form").addEventListener("submit", function (e) {
        e.preventDefault();
        var tickerInput = document.getElementById("add-ticker");
        var aboveInput = document.getElementById("alert-above");
        var belowInput = document.getElementById("alert-below");
        var errorEl = document.getElementById("add-error");

        var ticker = tickerInput.value.trim().toUpperCase();
        if (!ticker) return;

        // Check if already in watchlist
        var list = getWatchlist();
        if (list.find(function (i) { return i.ticker === ticker; })) {
            errorEl.textContent = ticker + " is already in your watchlist.";
            errorEl.classList.remove("hidden");
            return;
        }

        errorEl.classList.add("hidden");

        // Validate ticker via API
        var addBtn = document.getElementById("add-btn");
        addBtn.disabled = true;
        addBtn.textContent = "Adding...";

        fetch("/api/quote/" + ticker)
            .then(function (resp) {
                if (!resp.ok) throw new Error("Invalid ticker");
                return resp.json();
            })
            .then(function (data) {
                var alerts = {};
                if (aboveInput.value) alerts.above = parseFloat(aboveInput.value);
                if (belowInput.value) alerts.below = parseFloat(belowInput.value);

                list.push({
                    ticker: ticker,
                    name: data.name,
                    price: data.price,
                    change: data.change,
                    changePct: data.changePct,
                    alerts: (alerts.above != null || alerts.below != null) ? alerts : null,
                });
                saveWatchlist(list);
                renderWatchlist();

                tickerInput.value = "";
                aboveInput.value = "";
                belowInput.value = "";
            })
            .catch(function () {
                errorEl.textContent = "Could not find ticker '" + ticker + "'. Check the symbol.";
                errorEl.classList.remove("hidden");
            })
            .finally(function () {
                addBtn.disabled = false;
                addBtn.textContent = "Add";
            });
    });

    // Refresh button
    document.getElementById("refresh-btn").addEventListener("click", refreshAll);

    // Auto-refresh toggle
    var autoRefreshInterval = null;
    document.getElementById("auto-refresh").addEventListener("change", function () {
        if (this.checked) {
            refreshAll();
            autoRefreshInterval = setInterval(refreshAll, 60000);
        } else {
            if (autoRefreshInterval) clearInterval(autoRefreshInterval);
            autoRefreshInterval = null;
        }
    });

    // Initial render
    renderWatchlist();
})();
