"use strict";
(function () {
    const storageKey = "mm.lang";
    const defaultLang = "en";
    
    function readCookie(name) {
        try { return document.cookie.split(/;\s*/).map(x => x.split('=')).find(p => p[0] === name)?.[1] || null; } catch { return null; }
    }
    
    const current = (localStorage.getItem(storageKey) || readCookie('mm.lang') || defaultLang).toLowerCase();

    function apply(dict) {
        if (!dict || typeof dict !== 'object') {
            console.warn('[i18n] No translations available for', current);
            return;
        }
        
        console.log('[i18n] Applying translations for:', current, 'Keys:', Object.keys(dict).length);
        
        // direct data-i18n replacements
        document.querySelectorAll('[data-i18n]').forEach(el => {
            const key = el.getAttribute('data-i18n');
            if (!key) return;
            const value = dict[key];
            if (!value) return;
            if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
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

    function loadTranslations(lang) {
        // Load from local translations.js file
        return fetch('/assets/translations.js')
            .then(r => r.text())
            .then(code => {
                // Extract TRANSLATIONS object from the code
                const match = code.match(/const TRANSLATIONS = ({[\s\S]+?});/);
                if (!match) return {};
                const translationsObj = eval('(' + match[1] + ')');
                return translationsObj[lang] || translationsObj['en'] || {};
            })
            .catch(err => {
                console.error('[i18n] Failed to load translations:', err);
                return {};
            });
    }

    // Apply translations on load
    loadTranslations(current).then(apply);

    // Modern language switcher with globe icon and dropdown
    const langBtn = document.getElementById('lang-btn');
    const langPanel = document.getElementById('lang-panel');
    const langSwitcher = document.getElementById('lang-switcher');

    if (langBtn && langPanel) {
        // Toggle panel
        langBtn.addEventListener('click', function(e) {
            e.stopPropagation();
            const isOpen = langPanel.style.display !== 'none';
            langPanel.style.display = isOpen ? 'none' : 'block';
        });

        // Close on outside click
        document.addEventListener('click', function(e) {
            if (langSwitcher && !langSwitcher.contains(e.target)) {
                langPanel.style.display = 'none';
            }
        });

        // Set current language as active
        document.querySelectorAll('.lang-option').forEach(btn => {
            if (btn.getAttribute('data-lang') === current) {
                btn.classList.add('active');
            }
        });

        // Language option click handler
        document.querySelectorAll('.lang-option').forEach(btn => {
            btn.addEventListener('click', function() {
                const lang = this.getAttribute('data-lang') || defaultLang;
                localStorage.setItem(storageKey, lang);
                // also set cross-subdomain cookie if possible
                try {
                    const domain = window.location.hostname.includes('martoccimayhem.com') ? '.martoccimayhem.com' : undefined;
                    document.cookie = `mm.lang=${lang}; path=/; max-age=31536000${domain ? `; domain=${domain}` : ''}`;
                } catch { }
                
                // Load and apply new language translations
                loadTranslations(lang).then(apply);
                try { document.documentElement.setAttribute('lang', lang); } catch { }

                // Update active state
                document.querySelectorAll('.lang-option').forEach(b => b.classList.remove('active'));
                this.classList.add('active');

                // Close panel
                langPanel.style.display = 'none';
            });
        });
    }
})();