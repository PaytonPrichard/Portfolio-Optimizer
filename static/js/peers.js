// Async-load peer comparison data
document.addEventListener("DOMContentLoaded", function () {
    const container = document.getElementById("peers-content");
    if (!container) return;

    const ticker = container.dataset.ticker;
    if (!ticker) return;

    fetch("/api/peers/" + ticker)
        .then(function (resp) {
            if (!resp.ok) throw new Error("Failed to load peers");
            return resp.text();
        })
        .then(function (html) {
            container.innerHTML = html;
        })
        .catch(function () {
            container.innerHTML =
                '<p class="text-gray-500 italic p-4">Could not load peer comparison data.</p>';
        });
});
