/**
 * Authentication Utility
 * Provides token management and authenticated API calls
 */

// Token management
const AuthManager = {
    // Get stored access token
    getToken() {
        return localStorage.getItem('access_token');
    },

    // Get stored user info
    getUser() {
        const userStr = localStorage.getItem('user');
        return userStr ? JSON.parse(userStr) : null;
    },

    // Check if user is logged in
    isAuthenticated() {
        return !!this.getToken();
    },

    // Store token and user info
    setAuth(token, user) {
        localStorage.setItem('access_token', token);
        localStorage.setItem('user', JSON.stringify(user));
    },

    // Clear authentication data
    clearAuth() {
        localStorage.removeItem('access_token');
        localStorage.removeItem('user');
    },

    // Logout
    logout() {
        this.clearAuth();
        window.location.href = '/static/login.html';
    },

    // Get authorization headers
    getAuthHeaders() {
        const token = this.getToken();
        return token ? { 'Authorization': `Bearer ${token}` } : {};
    }
};

/**
 * Make an authenticated API call
 * @param {string} url - API endpoint URL
 * @param {object} options - Fetch options (method, body, etc.)
 * @returns {Promise} - Fetch promise
 */
async function authenticatedFetch(url, options = {}) {
    // Add authorization header
    const headers = {
        ...options.headers,
        ...AuthManager.getAuthHeaders()
    };

    // Make request
    const response = await fetch(url, {
        ...options,
        headers
    });

    // Handle 401 Unauthorized - token expired or invalid
    if (response.status === 401) {
        console.error('Authentication failed - redirecting to login');
        AuthManager.logout();
        throw new Error('Session expired. Please log in again.');
    }

    return response;
}

/**
 * Create user menu HTML
 * @returns {string} HTML string for user menu
 */
function createUserMenu() {
    if (!AuthManager.isAuthenticated()) {
        return `
            <div class="flex gap-3">
                <a href="/static/login.html" class="px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700 font-medium transition duration-150">Login</a>
                <a href="/static/signup.html" class="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 font-medium transition duration-150">Sign Up</a>
            </div>
        `;
    }

    const user = AuthManager.getUser();
    const userName = user ? user.name : 'User';

    return `
        <div class="relative" id="userMenuContainer">
            <button id="userMenuButton" class="flex items-center gap-2 px-4 py-2 bg-gray-100 rounded-md hover:bg-gray-200">
                <span class="font-medium text-gray-700">${userName}</span>
                <svg class="w-4 h-4 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path>
                </svg>
            </button>

            <!-- Dropdown Menu -->
            <div id="userDropdown" class="hidden absolute right-0 mt-2 w-48 bg-white rounded-md shadow-lg border border-gray-200 z-10">
                <div class="py-1">
                    <div class="px-4 py-2 text-sm text-gray-600 border-b border-gray-200">
                        ${user ? user.email : ''}
                    </div>
                    <a href="/static/search_list.html" class="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100">
                        My Properties
                    </a>
                    <button id="logoutButton" class="w-full text-left px-4 py-2 text-sm text-red-600 hover:bg-red-50">
                        Logout
                    </button>
                </div>
            </div>
        </div>
    `;
}

/**
 * Initialize user menu (call after DOM loaded)
 */
function initUserMenu() {
    // Wait a bit for menu to be rendered
    setTimeout(() => {
        const menuButton = document.getElementById('userMenuButton');
        const dropdown = document.getElementById('userDropdown');
        const logoutButton = document.getElementById('logoutButton');

        if (menuButton && dropdown) {
            // Toggle dropdown
            menuButton.addEventListener('click', (e) => {
                e.stopPropagation();
                dropdown.classList.toggle('hidden');
            });

            // Close dropdown when clicking outside
            document.addEventListener('click', () => {
                dropdown.classList.add('hidden');
            });

            // Prevent closing when clicking inside dropdown
            dropdown.addEventListener('click', (e) => {
                e.stopPropagation();
            });
        }

        if (logoutButton) {
            logoutButton.addEventListener('click', () => {
                AuthManager.logout();
            });
        }
    }, 100);
}

/**
 * Require authentication - redirect to login if not authenticated
 */
function requireAuth() {
    if (!AuthManager.isAuthenticated()) {
        window.location.href = '/static/login.html';
    }
}
