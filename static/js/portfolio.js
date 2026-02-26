// Tab switching for Upload CSV / Enter Manually
function switchPortfolioTab(tab) {
    var csvTab = document.getElementById("tab-csv");
    var manualTab = document.getElementById("tab-manual");
    var csvContent = document.getElementById("tab-content-csv");
    var manualContent = document.getElementById("tab-content-manual");
    if (!csvTab || !manualTab || !csvContent || !manualContent) return;

    var activeClasses = "border-brand text-brand";
    var inactiveClasses = "border-transparent text-gray-500 hover:text-gray-700";

    if (tab === "manual") {
        csvContent.classList.add("hidden");
        manualContent.classList.remove("hidden");
        csvTab.className = csvTab.className.replace(activeClasses, inactiveClasses);
        manualTab.className = manualTab.className.replace(inactiveClasses, activeClasses);
    } else {
        manualContent.classList.add("hidden");
        csvContent.classList.remove("hidden");
        manualTab.className = manualTab.className.replace(activeClasses, inactiveClasses);
        csvTab.className = csvTab.className.replace(inactiveClasses, activeClasses);
    }
}

// Portfolio CSV upload and analysis handler
// Security: PII columns (Account Number, Account Name) are stripped
// client-side BEFORE the file leaves the browser.
// Supports CSV exports from Fidelity, Schwab, Vanguard, E*Trade,
// TD Ameritrade, Robinhood, Interactive Brokers, Merrill Edge, and others.

// Columns we consider safe to send (financial data only, no PII).
// Includes naming variants across brokerages — the server normalizes them.
var SAFE_COLUMNS = [
    // Symbol
    "Symbol", "Ticker", "Ticker Symbol",
    // Description / Name
    "Description", "Name", "Security Name", "Security Description",
    "Investment Name", "Security", "Holding",
    // Quantity
    "Quantity", "Shares", "Qty", "Share Count",
    // Price
    "Last Price", "Price", "Share Price", "Close Price", "Closing Price",
    "Last", "Market Price", "Current Price",
    // Market Value
    "Current Value", "Market Value", "Total Value", "Value",
    "Mkt Value", "Account Value", "Equity",
    // Cost Basis
    "Cost Basis Total", "Cost Basis", "Total Cost", "Total Cost Basis",
    "Cost", "Book Value", "Purchase Value",
    // Cost Per Share
    "Average Cost Basis", "Cost Basis Per Share", "Avg Cost",
    "Average Cost", "Avg Cost/Share", "Avg Price", "Unit Cost",
    // Gain/Loss
    "Total Gain/Loss Dollar", "Gain/Loss Dollar", "Gain/Loss $",
    "Gain Loss $", "Unrealized Gain/Loss", "Unrealized P&L",
    "Gain/Loss", "P&L", "Total Gain/Loss",
    // Gain/Loss %
    "Total Gain/Loss Percent", "Gain/Loss Percent", "Gain/Loss %",
    "Gain Loss %", "Unrealized Gain/Loss %", "% Gain/Loss",
    // Weight
    "Percent Of Account", "% of Account", "% of Portfolio",
    "Weight", "Portfolio %", "Allocation", "Allocation %"
];

function parseCSVLine(line) {
    // Handle quoted fields with commas inside them
    var fields = [];
    var current = "";
    var inQuotes = false;
    for (var i = 0; i < line.length; i++) {
        var ch = line[i];
        if (ch === '"') {
            inQuotes = !inQuotes;
        } else if (ch === "," && !inQuotes) {
            fields.push(current.trim());
            current = "";
        } else {
            current += ch;
        }
    }
    fields.push(current.trim());
    return fields;
}

function stripSensitiveColumns(csvText) {
    // Parse CSV, keep only SAFE_COLUMNS, return new CSV string
    var lines = csvText.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");
    if (lines.length < 2) return null;

    // Strip trailing commas (some brokers add extra commas at end of rows)
    for (var t = 0; t < lines.length; t++) {
        lines[t] = lines[t].replace(/,+$/, "");
    }

    // Find header row (some CSVs have blank lines or metadata rows at top)
    var headerIdx = -1;
    var headers;
    for (var i = 0; i < Math.min(lines.length, 10); i++) {
        var parsed = parseCSVLine(lines[i]);
        // Look for a row that has a symbol-like column and a value-like column
        var hasSymbol = false;
        var hasValue = false;
        for (var c = 0; c < parsed.length; c++) {
            var col = parsed[c].trim();
            var colLower = col.toLowerCase();
            if (colLower === "symbol" || colLower === "ticker" || colLower === "ticker symbol") {
                hasSymbol = true;
            }
            if (colLower.indexOf("value") !== -1 || colLower.indexOf("quantity") !== -1 ||
                colLower === "shares" || colLower.indexOf("market") !== -1) {
                hasValue = true;
            }
        }
        if (hasSymbol && hasValue) {
            headerIdx = i;
            headers = parsed;
            break;
        }
    }
    if (headerIdx === -1) return null;

    // Build a case-insensitive lookup for safe columns
    var safeSet = {};
    for (var s = 0; s < SAFE_COLUMNS.length; s++) {
        safeSet[SAFE_COLUMNS[s].toLowerCase()] = SAFE_COLUMNS[s];
    }

    // Map safe column names to their indices in the original CSV
    var keepIndices = [];
    var keepNames = [];
    for (var h = 0; h < headers.length; h++) {
        var headerLower = headers[h].trim().toLowerCase();
        if (safeSet[headerLower]) {
            keepIndices.push(h);
            keepNames.push(headers[h].trim());
        }
    }

    if (keepIndices.length === 0) return null;

    // Rebuild CSV with only safe columns.
    // Stop at first empty line — many brokers append disclaimers after a blank line.
    var output = [keepNames.join(",")];
    var dataStarted = false;
    for (var r = headerIdx + 1; r < lines.length; r++) {
        if (!lines[r].trim()) {
            if (dataStarted) break;
            continue;
        }
        dataStarted = true;
        var fields = parseCSVLine(lines[r]);
        var row = [];
        for (var k = 0; k < keepIndices.length; k++) {
            var val = fields[keepIndices[k]] || "";
            // Re-quote if value contains commas
            if (val.indexOf(",") !== -1) {
                val = '"' + val + '"';
            }
            row.push(val);
        }
        output.push(row.join(","));
    }

    return output.join("\n");
}

document.addEventListener("DOMContentLoaded", function () {
    var form = document.getElementById("portfolio-form");
    var fileInput = document.getElementById("csv-input");
    var btn = document.getElementById("analyze-btn");
    var container = document.getElementById("portfolio-content");

    if (!form || !fileInput || !btn || !container) return;

    // Enable button when a file is selected
    fileInput.addEventListener("change", function () {
        btn.disabled = !fileInput.files.length;
    });

    form.addEventListener("submit", function (e) {
        e.preventDefault();

        if (!fileInput.files.length) return;

        btn.disabled = true;
        btn.textContent = "Stripping sensitive data...";

        // Show loading spinner
        container.innerHTML =
            '<div class="flex flex-col items-center justify-center py-16">' +
            '  <div class="w-12 h-12 border-4 border-gray-200 border-t-[#1F4E79] rounded-full animate-spin mb-4"></div>' +
            '  <p id="loading-status" class="text-sm font-medium text-brand">Stripping sensitive data...</p>' +
            '  <p class="text-xs text-gray-400 mt-1">Your account numbers are removed before anything is sent.</p>' +
            '</div>';

        // Read file client-side, strip PII, then upload only safe columns
        var reader = new FileReader();
        reader.onload = function (evt) {
            var rawCSV = evt.target.result;
            var safeCSV = stripSensitiveColumns(rawCSV);

            if (!safeCSV) {
                container.innerHTML =
                    '<p class="text-red-500 italic p-4">Could not parse CSV. Make sure this is a positions export from your brokerage with Symbol and value columns.</p>';
                btn.disabled = false;
                btn.textContent = "Analyze Portfolio";
                return;
            }

            btn.textContent = "Analyzing...";

            // Update loading status
            var status = document.getElementById("loading-status");
            if (status) {
                status.textContent = "Analyzing portfolio... This may take 15\u201330 seconds.";
            }

            // Send stripped CSV as a new file blob — no PII leaves the browser
            var blob = new Blob([safeCSV], { type: "text/csv" });
            var formData = new FormData();
            formData.append("csv", blob, "positions.csv");

            fetch("/api/portfolio/analyze", {
                method: "POST",
                body: formData,
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
                    // Init sort dropdowns and load async insight widgets.
                    // Wrapped in try/catch so a widget error can't nuke the results.
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
                            '<p class="text-red-500 italic p-4">Could not analyze portfolio. Please check your CSV file and try again.</p>';
                    }
                })
                .finally(function () {
                    btn.disabled = false;
                    btn.textContent = "Analyze Portfolio";
                });
        };

        reader.onerror = function () {
            container.innerHTML =
                '<p class="text-red-500 italic p-4">Could not read the file. Please try again.</p>';
            btn.disabled = false;
            btn.textContent = "Analyze Portfolio";
        };

        reader.readAsText(fileInput.files[0]);
    });
});
