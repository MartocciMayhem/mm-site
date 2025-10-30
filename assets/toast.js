/**
 * Martocci Mayhem Microsites - Toast Notification System
 * Lightweight toast notifications for user feedback
 * Version: 1.0.0
 */

(function () {
    'use strict';

    // Toast container
    let toastContainer = null;

    // Toast types with colors
    const TOAST_TYPES = {
        success: {
            bg: '#10b981',
            icon: '✓',
            label: 'Success'
        },
        error: {
            bg: '#ef4444',
            icon: '✕',
            label: 'Error'
        },
        warning: {
            bg: '#f59e0b',
            icon: '⚠',
            label: 'Warning'
        },
        info: {
            bg: '#3b82f6',
            icon: 'ℹ',
            label: 'Info'
        }
    };

    /**
     * Initialize toast container
     */
    function initToastContainer() {
        if (toastContainer) return;

        toastContainer = document.createElement('div');
        toastContainer.id = 'mm-toast-container';
        toastContainer.setAttribute('aria-live', 'polite');
        toastContainer.setAttribute('aria-atomic', 'true');

        // Styles
        Object.assign(toastContainer.style, {
            position: 'fixed',
            top: '20px',
            right: '20px',
            zIndex: '9999',
            display: 'flex',
            flexDirection: 'column',
            gap: '12px',
            maxWidth: '400px',
            pointerEvents: 'none'
        });

        document.body.appendChild(toastContainer);
    }

    /**
     * Create and show a toast notification
     * @param {Object} options - Toast options
     * @param {string} options.message - Toast message
     * @param {string} options.type - Toast type (success, error, warning, info)
     * @param {number} options.duration - Duration in milliseconds (default: 4000)
     * @param {boolean} options.dismissible - Whether toast can be dismissed (default: true)
     */
    function showToast(options) {
        if (!options || !options.message) {
            console.warn('[MMToast] Message is required');
            return null;
        }

        initToastContainer();

        const {
            message,
            type = 'info',
            duration = 4000,
            dismissible = true
        } = options;

        const config = TOAST_TYPES[type] || TOAST_TYPES.info;

        // Create toast element
        const toast = document.createElement('div');
        toast.className = 'mm-toast';
        toast.setAttribute('role', 'status');
        toast.setAttribute('aria-label', `${config.label}: ${message}`);

        // Styles
        Object.assign(toast.style, {
            display: 'flex',
            alignItems: 'flex-start',
            gap: '12px',
            padding: '12px 16px',
            background: config.bg,
            color: '#ffffff',
            borderRadius: '8px',
            boxShadow: '0 4px 12px rgba(0, 0, 0, 0.4)',
            fontSize: '14px',
            fontWeight: '500',
            lineHeight: '1.5',
            pointerEvents: 'auto',
            cursor: dismissible ? 'pointer' : 'default',
            transition: 'all 0.3s ease',
            transform: 'translateX(100%)',
            opacity: '0',
            maxWidth: '100%',
            wordWrap: 'break-word'
        });

        // Icon
        const icon = document.createElement('span');
        icon.textContent = config.icon;
        icon.style.fontSize = '18px';
        icon.style.flexShrink = '0';

        // Message
        const messageEl = document.createElement('span');
        messageEl.textContent = message;
        messageEl.style.flex = '1';

        // Close button (if dismissible)
        let closeBtn = null;
        if (dismissible) {
            closeBtn = document.createElement('button');
            closeBtn.innerHTML = '×';
            closeBtn.setAttribute('aria-label', 'Close notification');
            closeBtn.style.cssText = `
        background: transparent;
        border: none;
        color: rgba(255, 255, 255, 0.8);
        font-size: 20px;
        line-height: 1;
        padding: 0;
        margin-left: 8px;
        cursor: pointer;
        flex-shrink: 0;
        transition: color 0.2s;
      `;
            closeBtn.onmouseenter = () => closeBtn.style.color = '#ffffff';
            closeBtn.onmouseleave = () => closeBtn.style.color = 'rgba(255, 255, 255, 0.8)';
        }

        // Build toast
        toast.appendChild(icon);
        toast.appendChild(messageEl);
        if (closeBtn) toast.appendChild(closeBtn);

        // Add to container
        toastContainer.appendChild(toast);

        // Animate in
        requestAnimationFrame(() => {
            requestAnimationFrame(() => {
                toast.style.transform = 'translateX(0)';
                toast.style.opacity = '1';
            });
        });

        // Remove toast function
        const removeToast = () => {
            toast.style.transform = 'translateX(120%)';
            toast.style.opacity = '0';
            setTimeout(() => {
                if (toast.parentNode) {
                    toast.parentNode.removeChild(toast);
                }
                // Remove container if empty
                if (toastContainer && toastContainer.childNodes.length === 0) {
                    toastContainer.parentNode.removeChild(toastContainer);
                    toastContainer = null;
                }
            }, 300);
        };

        // Auto-dismiss
        let timeoutId = null;
        if (duration > 0) {
            timeoutId = setTimeout(removeToast, duration);
        }

        // Click to dismiss
        if (dismissible) {
            const handleDismiss = () => {
                if (timeoutId) clearTimeout(timeoutId);
                removeToast();
            };
            toast.addEventListener('click', handleDismiss);
            if (closeBtn) {
                closeBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    handleDismiss();
                });
            }
        }

        return {
            dismiss: removeToast,
            element: toast
        };
    }

    // Convenience methods
    const toast = {
        show: showToast,
        success: (message, options = {}) => showToast({ ...options, message, type: 'success' }),
        error: (message, options = {}) => showToast({ ...options, message, type: 'error' }),
        warning: (message, options = {}) => showToast({ ...options, message, type: 'warning' }),
        info: (message, options = {}) => showToast({ ...options, message, type: 'info' })
    };

    // Export to global scope
    window.MMToast = toast;

    // Also export as 'toast' for convenience
    if (!window.toast) {
        window.toast = toast;
    }

})();
