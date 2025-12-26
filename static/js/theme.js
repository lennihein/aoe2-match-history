document.addEventListener('DOMContentLoaded', () => {
    const toggleBtn = document.getElementById('themeToggle');
    const icon = toggleBtn ? toggleBtn.querySelector('.icon') : null;

    // Check saved theme
    const savedTheme = localStorage.getItem('theme');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    // Default to dark if no preference, or respect OS preference
    // Given the current site is dark by default, we'll assume dark unless 'light' is saved
    if (savedTheme === 'light') {
        document.documentElement.setAttribute('data-theme', 'light');
        if (icon) icon.textContent = '☀️';
    } else {
        document.documentElement.removeAttribute('data-theme');
        if (icon) icon.textContent = '🌙';
    }

    if (toggleBtn) {
        toggleBtn.addEventListener('click', () => {
            const currentTheme = document.documentElement.getAttribute('data-theme');
            if (currentTheme === 'light') {
                document.documentElement.removeAttribute('data-theme');
                localStorage.setItem('theme', 'dark');
                if (icon) icon.textContent = '🌙';
            } else {
                document.documentElement.setAttribute('data-theme', 'light');
                localStorage.setItem('theme', 'light');
                if (icon) icon.textContent = '☀️';
            }
        });
    }
});
