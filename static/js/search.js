// Search form enhancements
document.addEventListener("DOMContentLoaded", function () {
    var form = document.querySelector("form[action='/search']");
    if (!form) return;

    form.addEventListener("submit", function () {
        var btn = form.querySelector("button[type='submit']");
        if (btn) {
            btn.disabled = true;
            btn.textContent = "Searching...";
        }
    });
});
