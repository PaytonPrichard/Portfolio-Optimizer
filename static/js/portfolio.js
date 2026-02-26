// Portfolio CSV upload and analysis handler
// Security: PII columns (Account Number, Account Name) are stripped
// client-side BEFORE the file leaves the browser.

// Only these columns are sent to the server.
// Includes both known Fidelity naming variants for cost basis columns.
var SAFE_COLUMNS = [
    "Symbol", "Description", "Quantity", "Last Price", "Current Value",
    "Cost Basis Total", "Average Cost Basis",
    "Cost Basis", "Cost Basis Per Share",
    "Total Gain/Loss Dollar", "Total Gain/Loss Percent", "Percent Of Account"
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

    // Strip trailing commas — Fidelity rows end with "Cash," which creates
    // an extra empty field that shifts column alignment.
    for (var t = 0; t < lines.length; t++) {
        lines[t] = lines[t].replace(/,+$/, "");
    }

    // Find header row (Fidelity CSVs sometimes have blank lines at top)
    var headerIdx = -1;
    var headers;
    for (var i = 0; i < Math.min(lines.length, 5); i++) {
        var parsed = parseCSVLine(lines[i]);
        if (parsed.indexOf("Symbol") !== -1 && parsed.indexOf("Description") !== -1) {
            headerIdx = i;
            headers = parsed;
            break;
        }
    }
    if (headerIdx === -1) return null;

    // Map safe column names to their indices in the original CSV
    var keepIndices = [];
    var keepNames = [];
    for (var s = 0; s < SAFE_COLUMNS.length; s++) {
        var idx = headers.indexOf(SAFE_COLUMNS[s]);
        if (idx !== -1) {
            keepIndices.push(idx);
            keepNames.push(SAFE_COLUMNS[s]);
        }
    }

    if (keepIndices.length === 0) return null;

    // Rebuild CSV with only safe columns.
    // Stop at first empty line — Fidelity appends a legal disclaimer after a blank line.
    var output = [keepNames.join(",")];
    for (var r = headerIdx + 1; r < lines.length; r++) {
        if (!lines[r].trim()) break;
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
                    '<p class="text-red-500 italic p-4">Could not parse CSV. Make sure this is a Fidelity positions export with Symbol and Description columns.</p>';
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
