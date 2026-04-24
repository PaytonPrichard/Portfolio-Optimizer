// Sector Balance benchmark toggle for Diversification Insights.
//
// Uses document-level click delegation so it works when portfolio_results.html
// is injected via innerHTML (inline <script> tags inside injected HTML don't
// execute, per HTML5 spec).

(function () {
    var DESC_TEXT = {
        "market-cap": "Comparing your weights to the S&P 500's cap-weighted sector mix. Sector weights drift; numbers are approximate.",
        "equal-weight": "Comparing your weights to a naive equal-weight split (~9% per sector). Useful if you deliberately avoid the market's concentration.",
    };

    function fmtPp(pp) {
        if (Math.abs(pp) < 0.5) return "in line";
        return (pp > 0 ? "+" : "") + pp.toFixed(1) + "pp";
    }

    function updateToggle(container, mode) {
        container.querySelectorAll("[data-sector-bench]").forEach(function (b) {
            if (b.getAttribute("data-sector-bench") === mode) {
                b.className = "px-2.5 py-1 rounded-md bg-white dark:bg-gray-800 text-brand dark:text-blue-300 shadow-sm transition";
            } else {
                b.className = "px-2.5 py-1 rounded-md text-gray-600 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-200 transition";
            }
        });
        var desc = container.querySelector("[data-sector-bench-desc]");
        if (desc) desc.textContent = DESC_TEXT[mode];

        container.querySelectorAll("[data-sector-row]").forEach(function (row) {
            var targetAttr = mode === "market-cap" ? "data-market-target" : "data-equal-target";
            var diffAttr = mode === "market-cap" ? "data-market-diff" : "data-equal-diff";
            var target = parseFloat(row.getAttribute(targetAttr)) || 0;
            var diff = parseFloat(row.getAttribute(diffAttr)) || 0;
            var mark = row.querySelector("[data-sector-target-mark]");
            if (mark) mark.style.left = Math.min(target, 100) + "%";
            var diffEl = row.querySelector("[data-sector-diff]");
            if (diffEl) diffEl.textContent = fmtPp(diff);
        });
    }

    document.addEventListener("click", function (e) {
        var btn = e.target.closest("[data-sector-bench]");
        if (!btn) return;
        var container = btn.closest("[data-sector-balance]");
        if (!container) return;
        var mode = btn.getAttribute("data-sector-bench");
        updateToggle(container, mode);
    });
})();
