// Async-load peer comparison data
document.addEventListener("DOMContentLoaded", function () {
    const container = document.getElementById("peers-content");
    if (!container) return;

    const ticker = container.dataset.ticker;
    if (!ticker) return;

    var controller = new AbortController();
    var timeoutId = setTimeout(function () { controller.abort(); }, 30000);

    fetch("/api/peers/" + ticker, { signal: controller.signal })
        .then(function (resp) {
            if (!resp.ok) throw new Error("Failed to load peers");
            return resp.text();
        })
        .then(function (html) {
            container.innerHTML = html;
        })
        .catch(function (err) {
            if (err.name === "AbortError") {
                container.innerHTML =
                    '<p class="text-gray-500 italic p-4">Peer comparison request timed out. Please refresh the page to try again.</p>';
            } else {
                container.innerHTML =
                    '<p class="text-gray-500 italic p-4">Could not load peer comparison data.</p>';
            }
        })
        .finally(function () {
            clearTimeout(timeoutId);
        });
});
