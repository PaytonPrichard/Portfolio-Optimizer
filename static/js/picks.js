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
});
