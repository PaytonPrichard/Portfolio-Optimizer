// Manual portfolio entry — add/remove holdings, preview table, submit JSON.
// Complements portfolio.js (CSV upload) with a no-file-needed alternative.

(function () {
    var holdings = [];

    function showError(msg) {
        var el = document.getElementById("manual-error");
        if (!el) return;
        el.textContent = msg;
        el.classList.remove("hidden");
    }

    function hideError() {
        var el = document.getElementById("manual-error");
        if (el) el.classList.add("hidden");
    }

    function renderTable() {
        var tableDiv = document.getElementById("manual-holdings-table");
        var analyzeRow = document.getElementById("manual-analyze-row");
        if (!tableDiv) return;

        if (holdings.length === 0) {
            tableDiv.classList.add("hidden");
            tableDiv.innerHTML = "";
            if (analyzeRow) analyzeRow.classList.add("hidden");
            return;
        }

        var html =
            '<table class="w-full text-sm border border-gray-200 rounded-lg overflow-hidden">' +
            '<thead class="bg-gray-50"><tr>' +
            '<th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Symbol</th>' +
            '<th class="px-4 py-2 text-right text-xs font-medium text-gray-500 uppercase">Shares</th>' +
            '<th class="px-4 py-2 text-right text-xs font-medium text-gray-500 uppercase">Cost/Share</th>' +
            '<th class="px-4 py-2 text-center text-xs font-medium text-gray-500 uppercase w-16"></th>' +
            '</tr></thead><tbody>';

        for (var i = 0; i < holdings.length; i++) {
            var h = holdings[i];
            var costDisplay = h.costPerShare != null ? "$" + h.costPerShare.toFixed(2) : "--";
            html +=
                '<tr class="border-t border-gray-100">' +
                '<td class="px-4 py-2 font-medium">' + h.symbol + '</td>' +
                '<td class="px-4 py-2 text-right">' + h.shares + '</td>' +
                '<td class="px-4 py-2 text-right text-gray-500">' + costDisplay + '</td>' +
                '<td class="px-4 py-2 text-center">' +
                '<button type="button" data-remove="' + i + '" class="text-gray-400 hover:text-red-500 text-xs transition">Remove</button>' +
                '</td></tr>';
        }

        html += '</tbody></table>';
        tableDiv.innerHTML = html;
        tableDiv.classList.remove("hidden");
        if (analyzeRow) analyzeRow.classList.remove("hidden");

        // Event delegation for remove buttons
        tableDiv.onclick = function (e) {
            var btn = e.target.closest("[data-remove]");
            if (!btn) return;
            var idx = parseInt(btn.getAttribute("data-remove"), 10);
            holdings.splice(idx, 1);
            renderTable();
        };
    }

    // Exposed globally for onclick in template
    window.addHolding = function () {
        hideError();
        var symInput = document.getElementById("manual-symbol");
        var sharesInput = document.getElementById("manual-shares");
        var costInput = document.getElementById("manual-cost");
        if (!symInput || !sharesInput) return;

        var symbol = symInput.value.trim().toUpperCase();
        var sharesVal = sharesInput.value.trim();
        var costVal = costInput ? costInput.value.trim() : "";

        // Validate symbol: 1-10 uppercase letters
        if (!symbol || !/^[A-Z]{1,10}$/.test(symbol)) {
            showError("Enter a valid ticker symbol (1\u201310 letters).");
            symInput.focus();
            return;
        }

        // Validate shares: positive number
        var shares = parseFloat(sharesVal);
        if (!sharesVal || isNaN(shares) || shares <= 0) {
            showError("Enter a positive number of shares.");
            sharesInput.focus();
            return;
        }

        // Validate cost: optional, non-negative
        var costPerShare = null;
        if (costVal !== "") {
            costPerShare = parseFloat(costVal);
            if (isNaN(costPerShare) || costPerShare < 0) {
                showError("Cost per share must be a non-negative number.");
                costInput.focus();
                return;
            }
        }

        // Consolidate duplicate symbols
        var found = false;
        for (var i = 0; i < holdings.length; i++) {
            if (holdings[i].symbol === symbol) {
                var oldShares = holdings[i].shares;
                var newShares = oldShares + shares;
                // Weighted-average cost
                if (costPerShare != null && holdings[i].costPerShare != null) {
                    holdings[i].costPerShare = Math.round(
                        (holdings[i].costPerShare * oldShares + costPerShare * shares) / newShares * 100
                    ) / 100;
                } else if (costPerShare != null) {
                    holdings[i].costPerShare = costPerShare;
                }
                holdings[i].shares = newShares;
                found = true;
                break;
            }
        }

        if (!found) {
            holdings.push({ symbol: symbol, shares: shares, costPerShare: costPerShare });
        }

        renderTable();

        // Clear inputs and focus symbol for fast keyboard entry
        symInput.value = "";
        sharesInput.value = "";
        if (costInput) costInput.value = "";
        symInput.focus();
    };

    window.clearAllHoldings = function () {
        holdings = [];
        renderTable();
        hideError();
    };

    window.analyzeManual = function () {
        if (holdings.length === 0) return;
        hideError();

        var btn = document.getElementById("manual-analyze-btn");
        var container = document.getElementById("portfolio-content");
        if (!btn || !container) return;

        btn.disabled = true;
        btn.textContent = "Analyzing...";

        // Show loading spinner
        container.innerHTML =
            '<div class="flex flex-col items-center justify-center py-16">' +
            '  <div class="w-12 h-12 border-4 border-gray-200 border-t-[#1F4E79] rounded-full animate-spin mb-4"></div>' +
            '  <p class="text-sm font-medium text-brand">Analyzing portfolio... This may take 15\u201330 seconds.</p>' +
            '  <p class="text-xs text-gray-400 mt-1">Fetching live prices and enrichment data.</p>' +
            '</div>';

        fetch("/api/portfolio/analyze-manual", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(holdings),
        })
            .then(function (resp) {
                if (!resp.ok) {
                    return resp.text().then(function (html) {
                        container.innerHTML = html;
                        throw new Error("Analysis failed");
                    });
                }
                return resp.text();
            })
            .then(function (html) {
                container.innerHTML = html;
                container.scrollIntoView({ behavior: "smooth", block: "start" });
                try {
                    if (typeof initTableSorting === "function") {
                        initTableSorting();
                    }
                    if (typeof loadPortfolioWidgets === "function") {
                        loadPortfolioWidgets();
                    }
                } catch (widgetErr) {
                    console.error("Widget init error (results still shown):", widgetErr);
                }
            })
            .catch(function (err) {
                if (!container.innerHTML.includes("text-red-500")) {
                    container.innerHTML =
                        '<p class="text-red-500 italic p-4">Could not analyze portfolio. Please check your entries and try again.</p>';
                }
            })
            .finally(function () {
                btn.disabled = false;
                btn.textContent = "Analyze Portfolio";
            });
    };

    // Enter key in any manual input triggers Add
    document.addEventListener("DOMContentLoaded", function () {
        var ids = ["manual-symbol", "manual-shares", "manual-cost"];
        for (var i = 0; i < ids.length; i++) {
            var el = document.getElementById(ids[i]);
            if (el) {
                el.addEventListener("keydown", function (e) {
                    if (e.key === "Enter") {
                        e.preventDefault();
                        window.addHolding();
                    }
                });
            }
        }
    });
})();
