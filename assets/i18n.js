"use strict";
(function () {
    const APP_HOST = "https://app.martoccimayhem.com"; // production app origin
    const selectId = "mm-lang-switch";
    const storageKey = "mm.lang";
    const defaultLang = "en";
    function readCookie(name) {
        try { return document.cookie.split(/;\s*/).map(x => x.split('=')).find(p => p[0] === name)?.[1] || null; } catch { return null; }
    }
    const current = (localStorage.getItem(storageKey) || readCookie('mm.lang') || defaultLang).toLowerCase();

    function apply(dict) {
        if (!dict) return;
        // direct data-i18n replacements
        document.querySelectorAll('[data-i18n]').forEach(el => {
            const key = el.getAttribute('data-i18n');
            if (!key) return;
            const value = dict[key];
            if (!value) return;
            if (el.tagName === 'INPUT') {
                el.setAttribute('placeholder', value);
            } else {
                el.textContent = value;
            }
        });
        // category chip 'All'
        if (dict.category_all) {
            document.querySelectorAll('#filters button').forEach(b => {
                if (b.textContent.trim() === 'All') b.textContent = dict.category_all;
            });
        }
        // meta labels inside generated cards
        ['.meta', '.mini-meta'].forEach(sel => {
            document.querySelectorAll(sel).forEach(el => {
                if (dict.views_label) el.innerHTML = el.innerHTML.replace(/Views:/g, dict.views_label);
                if (dict.likes_label) el.innerHTML = el.innerHTML.replace(/Likes:/g, dict.likes_label);
            });
        });
    }

    function fetchDict(lang) {
        return fetch(APP_HOST + '/api/i18n/' + lang, { credentials: 'omit' })
            .then(r => r.ok ? r.json() : null)
            .then(j => j && j.dict || {})
            .catch(() => ({}));
    }

    fetchDict(current).then(apply);

    const sel = document.getElementById(selectId);
    if (sel) {
        sel.value = current;
        sel.addEventListener('change', e => {
            const lang = (e.target.value || defaultLang).toLowerCase();
            localStorage.setItem(storageKey, lang);
            // also set cross-subdomain cookie if possible
            try {
                const domain = window.location.hostname.includes('martoccimayhem.com') ? '.martoccimayhem.com' : undefined;
                document.cookie = `mm.lang=${lang}; path=/; max-age=31536000${domain ? `; domain=${domain}` : ''}`;
            } catch { }
            fetchDict(lang).then(apply);
            try { document.documentElement.setAttribute('lang', lang); } catch { }
        });
    }
})();