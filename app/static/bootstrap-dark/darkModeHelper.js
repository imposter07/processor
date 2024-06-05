const darkSwitch = document.getElementById('darkSwitch');
window.addEventListener('load', () => {
    if (darkSwitch) {
        initTheme();
        darkSwitch.addEventListener('change', () => {
            resetTheme();
        });
    }
});

function setIcon() {
    let iconVal = (darkSwitch.checked) ? 'bi-moon-fill': 'bi-sun-fill';
    let iconElem = document.getElementById('darkSwitchIcon');
    iconElem.classList.remove('bi-moon-fill', 'bi-sun-fill');
    iconElem.classList.add(iconVal);
}

function initTheme() {
    const darkThemeSelected =
        localStorage.getItem('darkSwitch') !== null &&
        localStorage.getItem('darkSwitch') === 'dark';
    darkSwitch.checked = darkThemeSelected;
    darkThemeSelected ? document.documentElement.setAttribute('data-bs-theme', 'dark') :
        document.documentElement.removeAttribute('data-bs-theme');
    setIcon();
}


/**
 * Summary: resetTheme checks if the switch is 'on' or 'off' and if it is toggled
 * on it will set the HTML attribute 'data-theme' to dark so the dark-theme CSS is
 * applied.
 * @return {void}
 */
function resetTheme() {
    if (darkSwitch.checked) {
        document.documentElement.setAttribute('data-bs-theme', 'dark');
        localStorage.setItem('darkSwitch', 'dark');
    } else {
        document.documentElement.removeAttribute('data-bs-theme');
        localStorage.removeItem('darkSwitch');
    }
    setIcon();
}