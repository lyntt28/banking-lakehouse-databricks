// Auto-detect server IP for cross-machine access (works with file:// and offline)
const API_BASE_URL = (() => {
    const isFile = window.location.protocol === 'file:';
    const hostname = window.location.hostname || 'localhost';
    const protocol = isFile ? 'http:' : window.location.protocol;
    return `${protocol}//${hostname}:8211`;
})();

// Global variables - CHANGED: Default product type to 'unsecured'
let selectedBranch = 'All';
let selectedDate = null;
let selectedProductType = 'All'; // Always use All - no product type filtering
let selectedChartType = 'monthly'; // New variable for chart type selection
let charts = {};
let currentData = null;
let branchNameMapping = {}; // Store branch name mapping

// Updated color palette matching the modern dashboard theme
const THEME_COLORS = {
    blue: '#5DADE2',        // Light blue for primary elements
    green: '#58D68D',       // Green for positive values and success
    orange: '#F39C12',      // Orange for highlights and warnings
    purple: '#BB8FCE',      // Purple for secondary elements
    cyan: '#48CAE4',        // Cyan for data visualization
    yellow: '#F7DC6F',      // Yellow for highlights
    red: '#EC7063',         // Red for negative values and losses
    darkBlue: '#3498DB',    // Darker blue for emphasis
    darkGreen: '#27AE60',   // Darker green for emphasis
    darkOrange: '#E67E22',  // Darker orange for emphasis
    darkPurple: '#8E44AD',  // Darker purple for emphasis
    darkCyan: '#17A2B8',    // Darker cyan for emphasis
    // Additional colors from the reference image
    lightBlue: '#AED6F1',   // Very light blue for backgrounds
    lightGreen: '#A9DFBF',  // Very light green for backgrounds
    lightOrange: '#F8C471', // Very light orange for backgrounds
    lightRed: '#F1948A',    // Very light red for backgrounds
    // Gradient colors for modern look
    gradientBlue: '#3498DB', // Primary blue for gradients
    gradientGreen: '#2ECC71', // Primary green for gradients
    gradientOrange: '#E67E22', // Primary orange for gradients
    gradientPurple: '#9B59B6', // Primary purple for gradients
    gradientRed: '#E74C3C',   // Primary red for gradients
    gradientCyan: '#17A2B8'   // Primary cyan for gradients
};

// Ensure ChartDataLabels plugin is properly registered
document.addEventListener('DOMContentLoaded', function() {
    // Wait for Chart.js and ChartDataLabels to be available
    const checkAndRegisterPlugin = () => {
        if (window.Chart && window.Chart.register && window.ChartDataLabels) {
            try {
                Chart.register(window.ChartDataLabels);
                console.log('ChartDataLabels plugin registered successfully');
            } catch (error) {
                console.error('Error registering ChartDataLabels plugin:', error);
            }
        } else {
            // Retry after a short delay
            setTimeout(checkAndRegisterPlugin, 100);
        }
    };
   
    checkAndRegisterPlugin();
});

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    setupDatePicker();
   
    document.querySelectorAll('.date-box, .branch-box').forEach(button => {
        button.addEventListener('click', function() {
            if (this.classList.contains('get-data')) {
                loadDashboardData();
            } else {
                this.classList.toggle('active');
            }
        });
    });
   
    // OPTIMIZED: Filter change handlers - only reload when filters actually change
   
    const branchSelect = document.getElementById('branch-select');
    if (branchSelect) {
        branchSelect.addEventListener('change', function() {
            const newBranch = this.value;
            if (newBranch !== selectedBranch) {
                selectedBranch = newBranch;
            console.log('Branch changed to:', selectedBranch);
            currentData = null;
            loadDashboardData();
            }
        });
    }

    const chartTypeSelect = document.getElementById('chart-type-select');
    if (chartTypeSelect) {
        chartTypeSelect.addEventListener('change', function() {
            const newChartType = this.value;
            if (newChartType !== selectedChartType) {
                selectedChartType = newChartType;
            console.log('Chart type changed to:', selectedChartType);
            currentData = null;
            loadDashboardData();
            }
        });
    }
   
    loadAvailableBranches();
});

// ENHANCED: Date picker with change detection
async function setupDatePicker() {
    const input = document.getElementById('date-picker');
    if (!input) return;
   
    let availableDates = [];
    try {
        console.log('Loading available dates...');
        const res = await fetch(`${API_BASE_URL}/available-dates`);
        if (!res.ok) {
            throw new Error(`HTTP error! status: ${res.status}`);
        }
        availableDates = await res.json();
        console.log('Available dates loaded:', availableDates);
        console.log('Total dates count:', availableDates.length);
       
        // FIXED: Check if dates ending with 31 are included
        const dates31 = availableDates.filter(date => date.endsWith('-31'));
        console.log('Dates ending with 31:', dates31);
    } catch (e) {
        console.error('Error loading available dates:', e);
        alert('❌ Cannot load available dates. Please check server connection.');
        return; // Dừng lại nếu không load được dates
    }
   
    if (window.datePickerInstance) {
        try {
            window.datePickerInstance.destroy();
        } catch(e) {
            console.log('Date picker instance cleanup');
        }
    }
   
    window.datePickerInstance = flatpickr(input, {
        dateFormat: 'Y-m-d',
        enable: availableDates,
        allowInput: false, // QUAN TRỌNG: Không cho nhập tay để tránh chọn ngày không có data
        onChange: function(selectedDates, dateStr) {
            if (dateStr && dateStr !== selectedDate) { // Only reload if date actually changed
                console.log('Date changed to:', dateStr);
                input.value = dateStr;
                selectedDate = dateStr;
                currentData = null;
                loadDashboardData();
            }
        }
    });
   
    // CHANGED: Set default date to latest available date and load data automatically
    if (availableDates.length > 0) {
        const initialDate = availableDates[0]; // Latest date (first in DESC order)
        input.value = initialDate;
        selectedDate = initialDate;
        console.log('Initial date set:', selectedDate);
        loadDashboardData(); // Auto-load data on page load
    } else {
        alert('⚠️ No data available in database.');
        console.warn('No available dates found');
        return;
    }
}

function refreshData() {
    console.log('refreshData called');
    // FIXED: Clear current data on refresh to ensure fresh data loading
    currentData = null;
    loadDashboardData();
}

// ENHANCED: Complete chart cleanup and proper canvas handling
function destroyAllCharts() {
    console.log('Destroying all existing charts...');
   
    // Destroy Chart.js instances
    Object.entries(charts).forEach(([key, chart]) => {
        if (chart && typeof chart.destroy === 'function') {
            try {
                chart.destroy();
                console.log(`Chart ${key} destroyed`);
            } catch (e) {
                console.warn(`Warning destroying chart ${key}:`, e);
            }
        }
    });
   
    // Clear charts object
    charts = {};
   
    // Clear all canvas contexts and force reset
    const canvasIds = ['debtGroupChart', 'dailyTrendChart', 'productGroupsChart', 'branchesChart', 'treemapChart', 'stackedBarChart'];
    canvasIds.forEach(canvasId => {
        const canvas = document.getElementById(canvasId);
        if (canvas) {
            try {
                const ctx = canvas.getContext('2d');
                if (ctx) {
                    ctx.clearRect(0, 0, canvas.width, canvas.height);
                }
                // Force canvas reset by reassigning dimensions
                const { width, height } = canvas.getBoundingClientRect();
                canvas.width = width;
                canvas.height = height;
                console.log(`Canvas ${canvasId} cleared and reset`);
            } catch (e) {
                console.warn(`Warning clearing canvas ${canvasId}:`, e);
            }
        }
    });
   
    console.log('Chart cleanup completed');
}

// SIMPLIFIED: Main dashboard loading - clean parallel execution
async function loadDashboardData() {
    try {
        console.log('Loading dashboard data with filters:', {
            date: selectedDate,
            productType: selectedProductType,
            branch: selectedBranch
        });
       
        // Clean slate
        destroyAllCharts();
        await new Promise(resolve => setTimeout(resolve, 100));
       
        updateChartTitles();

        // Show loading states for trend charts
        showChartLoading('debtGroupChart', 'Monthly Trend');
        showChartLoading('dailyTrendChart', 'Daily Trend');

        // Load branch mapping first
        await loadBranchNameMapping();
       
        // Load all charts in parallel - each uses single optimized API
        const chartPromises = [
            loadTickers(),
            loadProductGroupsChart(),
            loadBranchesChart(),
            loadTreemapChart(),
            loadStackedBarChart()
        ];

        // Load quick charts first
        await Promise.all(chartPromises);

        // Load trend charts (these are the expensive ones)
        await Promise.all([
            loadDebtGroupChart(),
            loadDailyTrendChart()
        ]);
       
        console.log('Dashboard data loaded successfully');
    } catch (error) {
        console.error('Error loading dashboard data:', error);
    }
}

// Load branch name mapping
async function loadBranchNameMapping() {
    try {
        branchNameMapping = await fetchAPI('/branch-mapping');
        console.log('Branch name mapping loaded:', branchNameMapping);
    } catch (error) {
        console.error('Error loading branch name mapping:', error);
        branchNameMapping = {}; // Fallback to empty mapping
    }
}

function updateChartTitles() {
    const debtGroupTitle = document.getElementById('debtGroupChartTitle');
    const dailyTrendTitle = document.getElementById('dailyTrendChartTitle');
   
    if (debtGroupTitle) {
        debtGroupTitle.textContent = `Overdue Trend (Monthly)`;
    }
   
    if (dailyTrendTitle) {
        dailyTrendTitle.textContent = `Overdue Trend (Last 5 Days)`;
    }
}

async function loadAvailableBranches() {
    try {
        console.log('Loading available branches from database...');
        
        // Load both available branches and branch mapping
        const [branches, branchMapping] = await Promise.all([
            fetchAPI('/available-branches'),
            fetchAPI('/branch-mapping')
        ]);
        
        const branchSelect = document.getElementById('branch-select');
       
        if (branchSelect && branches.length > 0) {
            branchSelect.innerHTML = '<option value="All" selected style="background: #4299e1; color: #fff;">All</option>';

            // Build a unique map by normalized code → preferred raw code and display name
            const normalizedToEntry = new Map();

            branches.forEach(branchCode => {
                // Normalize code (e.g., "8120.0" → "8120")
                let normalizedCode = String(branchCode);
                if (normalizedCode.includes('.')) {
                    const floatVal = parseFloat(normalizedCode);
                    if (!isNaN(floatVal) && floatVal % 1 === 0) {
                        normalizedCode = String(Math.floor(floatVal));
                    }
                }

                // Prefer non-decimal raw code value when duplicates exist
                const isDecimalForm = String(branchCode).includes('.') && String(branchCode).endsWith('.0');
                const existing = normalizedToEntry.get(normalizedCode);
                if (!existing) {
                    normalizedToEntry.set(normalizedCode, { rawValue: String(branchCode), isDecimalForm });
                } else {
                    // If current is non-decimal and existing is decimal, replace
                    if (!isDecimalForm && existing.isDecimalForm) {
                        normalizedToEntry.set(normalizedCode, { rawValue: String(branchCode), isDecimalForm });
                    }
                }
            });

            // Append options once per normalized code
            normalizedToEntry.forEach((entry, normalizedCode) => {
                const option = document.createElement('option');
                // Standardize: always use normalized string value for API calls
                option.value = String(normalizedCode);

                // Display readable branch name (Title Case), fallback to normalized code
                const rawName = branchMapping[normalizedCode] || normalizedCode;
                const branchName = rawName.toLowerCase().replace(/\b\w/g, l => l.toUpperCase());
                option.textContent = branchName;

                option.style.background = '#4299e1';
                option.style.color = '#fff';
                branchSelect.appendChild(option);

                console.log(`Added branch option: "${branchName}" (value="${normalizedCode}")`);
            });
            
            console.log(`✅ Loaded ${branches.length} branches from database:`, branches);
            console.log('✅ Branch mapping applied for readable names with de-duplication');
        } else {
            console.warn('⚠️ No branches found in database or branch select element not found');
        }
    } catch (error) {
        console.error('❌ Error loading available branches:', error);
        // Keep default "All" option
        const branchSelect = document.getElementById('branch-select');
        if (branchSelect) {
            branchSelect.innerHTML = '<option value="All" selected style="background: #4299e1; color: #fff;">All</option>';
        }
    }
}

async function loadTickers() {
    try {
        let ticker1, ticker2, ticker3;
        // Always use All - no product type filtering, just get all data
        const params = new URLSearchParams({ product_type: 'All', branch: selectedBranch });
        if (selectedDate) {
            params.append('date', selectedDate);
        }
       
        const [t1, t2, t3] = await Promise.all([
            fetchAPI(`/ticker1?${params.toString()}`),
            fetchAPI(`/ticker2_filtered?${params.toString()}`),
            fetchAPI(`/ticker3_filtered?${params.toString()}`)
        ]);
       
        ticker1 = t1;
        ticker2 = t2;
        ticker3 = t3;
       
        updateStatCardWithComparisons(0, ticker1.value, ticker1.unit || '%', ticker1.comparisons);
        updateStatCardWithComparisons(1, ticker2.value, ticker2.unit || 'USD', ticker2.comparisons);
        updateStatCardWithComparisons(2, ticker3.value, ticker3.unit || 'USD', ticker3.comparisons);
        console.log('Tickers loaded successfully');
    } catch (error) {
        console.error('Error loading tickers:', error);
    }
}

function combineComparisons(ratioComps, overdueComps, endingComps) {
    const periods = ['last_day', 'last_month', 'last_year'];
    const result = { ratio: {}, overdue: {}, ending: {} };
   
    periods.forEach(period => {
        const overdueSum = (overdueComps[0][period]?.value || 0) + (overdueComps[1][period]?.value || 0);
        const overdueChange = (overdueComps[0][period]?.change || 0) + (overdueComps[1][period]?.change || 0);
       
        const endingSum = (endingComps[0][period]?.value || 0) + (endingComps[1][period]?.value || 0);
        const endingChange = (endingComps[0][period]?.change || 0) + (endingComps[1][period]?.change || 0);
       
        const currentRatio = endingSum > 0 ? (overdueSum / endingSum * 100) : 0;
        const prevOverdueSum = overdueSum - overdueChange;
        const prevEndingSum = endingSum - endingChange;
        const prevRatio = prevEndingSum > 0 ? (prevOverdueSum / prevEndingSum * 100) : 0;
        const ratioChange = currentRatio - prevRatio;
       
        result.ratio[period] = {
            value: prevRatio,
            change: ratioChange,
            date: overdueComps[0][period]?.date || overdueComps[1][period]?.date
        };
       
        result.overdue[period] = {
            value: prevOverdueSum,
            change: overdueChange,
            date: overdueComps[0][period]?.date || overdueComps[1][period]?.date
        };
       
        result.ending[period] = {
            value: prevEndingSum,
            change: endingChange,
            date: endingComps[0][period]?.date || endingComps[1][period]?.date
        };
    });
   
    return result;
}

// SIMPLIFIED: Monthly Trend Chart - Single API call
async function loadDebtGroupChart() {
    try {
        console.log('Loading Monthly chart...');
       
        const canvas = document.getElementById('debtGroupChart');
        if (!canvas) {
            console.error('Monthly chart canvas not found!');
            return;
        }
       
        // Clean up existing chart
        if (charts.debtGroupChart) {
            charts.debtGroupChart.destroy();
            charts.debtGroupChart = null;
        }
       
        // Build API endpoint based on current filter state
        const params = new URLSearchParams({ branch: selectedBranch });
        if (selectedDate) {
            params.append('date', selectedDate);
        }

        let endpoint;
        // Always use All - no product type filtering
        params.append('product_type', 'All');
        endpoint = `/bar_line_chart_optimized?${params.toString()}`;
       
        console.log('Monthly chart endpoint:', endpoint);
        const data = await fetchAPI(endpoint);
       
        // Create chart with received data
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
       
        const maxBarValue = Math.max(...data.bar_data);
        const maxLineValue = Math.max(...data.line_data);
        const yMaxBar = maxBarValue * 1.3; // Add 30%
        const yMaxLine = maxLineValue + 3; // Add 3%

        charts.debtGroupChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Overdue Amount (USD)',
                    data: data.bar_data,
                    backgroundColor: THEME_COLORS.gradientBlue,
                    borderRadius: 6,
                    datalabels: {
                        color: THEME_COLORS.gradientBlue,
                        font: { size: 18, weight: 'bold' },
                        anchor: 'end',
                        align: 'top',
                        offset: 8,
                        formatter: function(value) {
                            return Math.round(value / 1000).toLocaleString('en-US');
                        },
                        display: function(context) {
                            return context.dataset.data[context.dataIndex] > 0;
                        }
                    },
                    order: 2
                }, {
                    label: 'Overdue Ratio (%)',
                    data: data.line_data,
                    backgroundColor: THEME_COLORS.gradientOrange,
                    borderColor: THEME_COLORS.gradientOrange,
                    borderWidth: 3,
                    type: 'line',
                    yAxisID: 'y1',
                    tension: 0.4,
                    fill: false,
                    pointBackgroundColor: THEME_COLORS.gradientOrange,
                    pointBorderColor: '#fff',
                    pointRadius: 4,
                    order: 1,
                    datalabels: {
                        color: THEME_COLORS.gradientOrange,
                        font: { size: 18, weight: 'bold' },
                        anchor: 'end',
                        align: 'top',
                        offset: 8,
                        formatter: function(value) {
                            return value.toFixed(2) + '%';
                        },
                        display: function(context) {
                            return context.dataset.data[context.dataIndex] > 0;
                        }
                    }
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            usePointStyle: true,
                            padding: 15,
                            font: { size: 14 },
                            color: '#1a202c'
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        max: yMaxBar,
                        position: 'left',
                        grid: { color: 'rgba(203, 213, 224, 0.3)' },
                        ticks: {
                            color: '#1a202c',
                            font: { size: 14 },
                            callback: function(value) {
                                return Math.round(value / 1000) + 'K';
                            }
                        }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        max: yMaxLine,
                        position: 'right',
                        grid: { drawOnChartArea: false },
                        ticks: {
                            color: '#1a202c',
                            font: { size: 14 },
                            callback: function(value) {
                                return value.toFixed(2) + '%';
                            }
                        }
                    },
                    x: {
                        grid: { color: 'rgba(203, 213, 224, 0.3)' },
                        ticks: {
                            color: '#1a202c',
                            font: { size: 14 }
                        }
                    }
                },
                layout: { padding: { left: 15, right: 15, top: 10, bottom: 10 } }
            },
            plugins: [window.ChartDataLabels]
        });

        console.log('Monthly chart loaded successfully');
        hideChartLoading('debtGroupChart');
    } catch (error) {
        console.error('Error loading monthly chart:', error);
        showChartError('debtGroupChart', 'Error loading monthly chart: ' + error.message);
        hideChartLoading('debtGroupChart');
    }
}

// SIMPLIFIED: Daily Trend Chart - Single API call
async function loadDailyTrendChart() {
    try {
        console.log('Loading Daily trend chart...');
       
        const canvas = document.getElementById('dailyTrendChart');
        if (!canvas) {
            console.error('Daily trend chart canvas not found!');
            return;
        }

        // Clean up existing chart
        if (charts.dailyTrendChart) {
            charts.dailyTrendChart.destroy();
            charts.dailyTrendChart = null;
        }

        // Build API endpoint based on current filter state
        const params = new URLSearchParams({ branch: selectedBranch });
        if (selectedDate) {
            params.append('date', selectedDate);
        }

        let endpoint;
        // Always use All - no product type filtering
        params.append('product_type', 'All');
        endpoint = `/bar_line_5days_optimized?${params.toString()}`;

        console.log('Daily trend chart endpoint:', endpoint);
        const data = await fetchAPI(endpoint);

        // Create chart with received data
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);

        const maxBarValue = Math.max(...data.bar_data);
        const maxLineValue = Math.max(...data.line_data);
        const yMaxBar = maxBarValue * 1.3; // Add 30%
        const yMaxLine = maxLineValue + 3; // Add 3%
       
        charts.dailyTrendChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: data.labels,
                    datasets: [{
                        label: 'Overdue Amount (USD)',
                        data: data.bar_data,
                        backgroundColor: THEME_COLORS.gradientBlue,
                        borderRadius: 6,
                        datalabels: {
                            color: THEME_COLORS.gradientBlue,
                        font: { size: 18, weight: 'bold' },
                            anchor: 'end',
                            align: 'top',
                            offset: 8,
                            formatter: function(value) {
                                return Math.round(value / 1000).toLocaleString('en-US');
                            },
                            display: function(context) {
                                return context.dataset.data[context.dataIndex] > 0;
                            }
                        },
                        order: 2
                    }, {
                        label: 'Overdue Ratio (%)',
                        data: data.line_data,
                        backgroundColor: THEME_COLORS.gradientOrange,
                        borderColor: THEME_COLORS.gradientOrange,
                        borderWidth: 3,
                        type: 'line',
                        yAxisID: 'y1',
                        tension: 0.4,
                        fill: false,
                        pointBackgroundColor: THEME_COLORS.gradientOrange,
                        pointBorderColor: '#fff',
                        pointRadius: 4,
                        order: 1,
                        datalabels: {
                            color: THEME_COLORS.gradientOrange,
                        font: { size: 18, weight: 'bold' },
                            anchor: 'end',
                            align: 'top',
                            offset: 8,
                            formatter: function(value) {
                                return value.toFixed(2) + '%';
                            },
                            display: function(context) {
                                return context.dataset.data[context.dataIndex] > 0;
                            }
                        }
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                usePointStyle: true,
                            padding: 15,
                            font: { size: 14 },
                                color: '#1a202c'
                            }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            max: yMaxBar,
                            position: 'left',
                            grid: { color: 'rgba(203, 213, 224, 0.3)' },
                            ticks: {
                                color: '#1a202c',
                            font: { size: 14 },
                                callback: function(value) {
                                    return Math.round(value / 1000) + 'K';
                                }
                            }
                        },
                        y1: {
                            type: 'linear',
                            display: true,
                            max: yMaxLine,
                            position: 'right',
                            grid: { drawOnChartArea: false },
                            ticks: {
                                color: '#1a202c',
                            font: { size: 14 },
                                callback: function(value) {
                                    return value.toFixed(2) + '%';
                                }
                            }
                        },
                        x: {
                            grid: { color: 'rgba(203, 213, 224, 0.3)' },
                            ticks: {
                                color: '#1a202c',
                            font: { size: 14 }
                            }
                        }
                    },
                    layout: { padding: { left: 15, right: 15, top: 10, bottom: 10 } }
                },
                plugins: [window.ChartDataLabels]
            });

        console.log('Daily trend chart loaded successfully');
        hideChartLoading('dailyTrendChart');
    } catch (error) {
        console.error('Error loading daily trend chart:', error);
        showChartError('dailyTrendChart', 'Error loading daily trend: ' + error.message);
        hideChartLoading('dailyTrendChart');
    }
}

// ENHANCED: Branches Chart (Product Groups) with proper error handling
async function loadBranchesChart() {
    try {
        const params = new URLSearchParams();
       
        if (selectedDate) {
            params.append('date', selectedDate);
        }
       
        if (selectedBranch && selectedBranch !== 'All') {
            params.append('branch', selectedBranch);
        }
       
        // Always use All - no product type filtering
        params.append('product_type', 'All');
       
        const queryString = params.toString();
        const endpoint = queryString ? `/overdue_vertical_bar?${queryString}` : '/overdue_vertical_bar';
       
        const data = await fetchAPI(endpoint);
        const canvas = document.getElementById('branchesChart');
        if (!canvas) return;
       
        if (charts.branchesChart) {
            charts.branchesChart.destroy();
        }
       
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
       
        // Updated gradient colors for product groups with modern palette - all blue
        const modernGradientColors = [
            THEME_COLORS.gradientBlue, THEME_COLORS.gradientBlue, THEME_COLORS.gradientBlue,
            THEME_COLORS.gradientBlue, THEME_COLORS.gradientBlue
        ];
       
        // Add 3% to max value instead of 5%
        const maxValue = Math.max(...data.data);
        const xMax = maxValue + 3; // Add 3%
       
        charts.branchesChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Overdue Ratio %',
                    data: data.data,
                    backgroundColor: modernGradientColors,
                    borderRadius: 6,
                    datalabels: {
                        display: true,
                        color: '#000000',
                        font: { size: 14, weight: 'bold' },
                        anchor: 'end',
                        align: 'end',
                        formatter: function(value) {
                            return value.toFixed(2) + '%';
                        }
                    }
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        max: xMax,
                        grid: { color: 'rgba(203, 213, 224, 0.3)' },
                        ticks: {
                            color: '#1a202c',
                            font: { size: 14 },
                            callback: function(value) {
                                return value.toFixed(2) + '%';
                            }
                        }
                    },
                    y: {
                        grid: { display: false },
                        ticks: {
                            color: '#1a202c',
                            font: { size: 14 }
                        }
                    }
                }
            },
            plugins: [window.ChartDataLabels]
        });
       
        console.log('Product Groups chart loaded successfully');
    } catch (error) {
        console.error('Error loading product groups chart:', error);
        showChartError('branchesChart', 'Error loading product groups: ' + error.message);
    }
}

// ENHANCED: Treemap with proper error handling and cleanup
async function loadTreemapChart() {
    try {
        const params = new URLSearchParams();
       
        if (selectedDate) {
            params.append('date', selectedDate);
        }
       
        if (selectedBranch && selectedBranch !== 'All') {
            params.append('branch', selectedBranch);
        }
       
        // Always use All - no product type filtering
        params.append('product_type', 'All');
       
        const queryString = params.toString();
        const endpoint = queryString ? `/overdue_treemap?${queryString}` : '/overdue_treemap';
       
        const data = await fetchAPI(endpoint);
       
        // Clear any existing treemap display
        const canvas = document.getElementById('treemapChart');
        const fallbackDiv = document.getElementById('fallback-treemap');
       
        if (canvas) canvas.style.display = 'none';
        if (fallbackDiv) fallbackDiv.innerHTML = '';
       
        createDynamicTreemap(data);
       
        console.log('Treemap chart loaded successfully');
    } catch (error) {
        console.error('Error loading treemap chart:', error);
        // Show error in treemap container
        const fallbackDiv = document.getElementById('fallback-treemap');
        if (fallbackDiv) {
            fallbackDiv.innerHTML = `
                <div style="display: flex; align-items: center; justify-content: center; color: #e53e3e; font-size: 16px; height: 100%; background: #fed7d7; border-radius: 8px; border: 2px dashed #fc8181;">
                    <div style="text-align: center;">
                        <div style="font-size: 24px; margin-bottom: 8px;">⚠️</div>
                        <div>Error loading treemap</div>
                        <div style="font-size: 14px; color: #c53030; margin-top: 4px;">${error.message}</div>
                    </div>
                </div>
            `;
            fallbackDiv.style.display = 'block';
        }
    }
}

// ENHANCED: Stacked Bar Chart with proper error handling
async function loadStackedBarChart() {
    try {
        const params = new URLSearchParams();
       
        if (selectedDate) {
            params.append('date', selectedDate);
        }
       
        if (selectedBranch && selectedBranch !== 'All') {
            params.append('branch', selectedBranch);
        }
       
        // Always use All - no product type filtering
        params.append('product_type', 'All');
       
        const queryString = params.toString();
        const endpoint = queryString ? `/overdue_stackbar?${queryString}` : '/overdue_stackbar';
       
        const data = await fetchAPI(endpoint);
        const canvas = document.getElementById('stackedBarChart');
        if (!canvas) return;
       
        if (charts.stackedBarChart) {
            charts.stackedBarChart.destroy();
        }
       
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
       
        // Updated stacked bar colors using blue gradient from light to dark
        const stackedColors = [
            '#E3F2FD', '#BBDEFB', '#90CAF9', '#64B5F6', '#42A5F5',
            '#2196F3', '#1E88E5', '#1976D2', '#1565C0', '#0D47A1'
        ];
       
        charts.stackedBarChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: data.datasets.map((dataset, index) => ({
                    label: dataset.label,
                    data: dataset.data,
                    backgroundColor: stackedColors[index % stackedColors.length],
                    borderColor: stackedColors[index % stackedColors.length],
                    borderWidth: 1,
                datalabels: {
                    display: true,
                    color: function(context) {
                        // 1-10, 11-90 tô đen, còn lại trắng
                        const datasetIndex = context.datasetIndex;
                        return (datasetIndex === 0 || datasetIndex === 1) ? '#000000' : '#FFFFFF';
                    },
                                            font: { size: 14, weight: 'bold' },
                    anchor: 'center',
                    align: 'center',
                    formatter: function(value) {
                        return value > 0 ? Math.round(value / 1000).toLocaleString('en-US') : '';
                    }
                }
                }))
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            usePointStyle: true,
                            padding: 20,
                            font: { size: 14 },
                            color: '#1a202c'
                        }
                    }
                },
                scales: {
                    x: {
                        stacked: true,
                        grid: { color: 'rgba(203, 213, 224, 0.3)' },
                        ticks: {
                            color: '#1a202c',
                            font: { size: 14 }
                        }
                    },
                    y: {
                        stacked: true,
                        beginAtZero: true,
                        grid: { color: 'rgba(203, 213, 224, 0.3)' },
                        ticks: {
                            color: '#1a202c',
                            font: { size: 14 },
                            callback: function(value) {
                                return Math.round(value / 1000) + 'K';
                        }
                    }
                }
                },
                layout: { padding: { left: 15, right: 15, top: 10, bottom: 10 } }
            },
            plugins: [window.ChartDataLabels]
        });
       
        console.log('Stacked bar chart loaded successfully');
    } catch (error) {
        console.error('Error loading stacked bar chart:', error);
        showChartError('stackedBarChart', 'Error loading stacked bar: ' + error.message);
    }
}

async function loadProductGroupsChart() {
    try {
        const params = new URLSearchParams();
        if (selectedDate) {
            params.append('date', selectedDate);
        }
       
        if (selectedBranch && selectedBranch !== 'All') {
            params.append('branch', selectedBranch);
        }
       
        // Always use All - no product type filtering
        params.append('product_type', 'All');
       
        const queryString = params.toString();
        const endpoint = queryString ? `/overdue_donut?${queryString}` : '/overdue_donut';
        const data = await fetchAPI(endpoint);
       
        const canvas = document.getElementById('productGroupsChart');
        if (!canvas) return;
       
        if (charts.productGroupsChart) {
            charts.productGroupsChart.destroy();
        }
       
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
       
        const total = data.data.reduce((sum, value) => sum + value, 0);
       
        // Different colors for each product group like Excel chart
        const modernColors = [
            '#3498DB', '#E74C3C', '#2ECC71', '#F39C12', '#9B59B6',
            '#1ABC9C', '#E67E22', '#34495E', '#E91E63', '#00BCD4'
        ];
           
        charts.productGroupsChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: data.labels,
                datasets: [{
                    data: data.data,
                    backgroundColor: modernColors.slice(0, data.labels.length),
                    borderWidth: 2,
                    borderColor: '#ffffff',
                    datalabels: {
                        display: true,
                        color: '#ffffff',
                        font: { size: 14, weight: 'bold' },
                        anchor: 'center',
                        align: 'center',
                        offset: 0,
                        clip: false,
                        clamp: true,
                        display: function(context) {
                            const data = context.chart.data.datasets[context.datasetIndex].data || [];
                            const value = data[context.dataIndex] || 0;
                           
                            // Create array of values with their indices to find top 3
                            const dataWithIndices = data.map((val, idx) => ({ value: val, index: idx }));
                            // Sort by value in descending order
                            dataWithIndices.sort((a, b) => b.value - a.value);
                           
                            // Get indices of top 3 largest values
                            const top3Indices = dataWithIndices.slice(0, 3).map(item => item.index);
                           
                            // Only show label if current index is in top 3
                            return top3Indices.includes(context.dataIndex);
                        },
                        formatter: function(value, context) {
                            return Math.round(value / 1000).toLocaleString('en-US');
                        },
                        backgroundColor: 'rgba(0, 0, 0, 0.7)',
                        borderColor: 'rgba(255, 255, 255, 0.3)',
                        borderWidth: 1,
                        borderRadius: 6,
                        padding: 6
                    }
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'right',
                        labels: {
                            usePointStyle: true,
                            padding: 20,
                            font: { size: 14 },
                            color: '#1a202c'
                        }
                    }
                },
                layout: { padding: { left: 15, right: 15, top: 10, bottom: 10 } }
            },
            plugins: [window.ChartDataLabels]
        });
       
        console.log('Product Groups chart loaded successfully');
    } catch (error) {
        console.error('Error loading product groups chart:', error);
        showChartError('productGroupsChart', 'Error loading product groups: ' + error.message);
    }
}

// ENHANCED: Dynamic treemap with better error handling and cleanup
function createVerticalBranchChart(data) {
    console.log('Creating vertical branch chart...');
   
    const canvas = document.getElementById('treemapChart');
    const fallbackDiv = document.getElementById('fallback-treemap');
   
    if (!fallbackDiv) {
        console.error('Chart container not found');
        return;
    }
   
    if (canvas) canvas.style.display = 'none';
    fallbackDiv.style.display = 'block';
   
    // Clear any existing content
    fallbackDiv.innerHTML = '';
   
    if (data && data.length > 0) {
        // Filter out specific branch and get top 5
        const excludedBranch = '8125';
        const filteredData = data.filter(item => String(item.key) !== excludedBranch);
        const sortedData = [...filteredData].sort((a, b) => b.value - a.value);
        let top5Data = sortedData.slice(0, 5);
       
        if (top5Data.length < 5) {
            const remainingBranches = sortedData.slice(5, 10);
            top5Data = [...top5Data, ...remainingBranches].slice(0, 5);
        }
       
        top5Data = top5Data.slice(0, 5);
       
        if (top5Data.length === 0) {
            fallbackDiv.innerHTML = `
                <div style="display: flex; align-items: center; justify-content: center; color: #e53e3e; font-size: 16px; height: 100%; background: #fed7d7; border-radius: 8px; border: 2px dashed #fc8181;">
                    <div style="text-align: center;">
                        <div style="font-size: 24px; margin-bottom: 8px;">⚠️</div>
                        <div>No branches available</div>
                    </div>
                </div>
            `;
            return;
        }
       
        // Map branch codes to branch names
        const mappedData = top5Data.map(item => ({
            ...item,
            displayKey: (branchNameMapping[item.key] || item.key).toUpperCase()
        }));
       
        // Create vertical bar chart container
        fallbackDiv.innerHTML = `
            <div style="width: 100%; height: 100%; padding: 10px;">
                <canvas id="branchVerticalChart" style="width: 100%; height: 100%;"></canvas>
            </div>
        `;
       
        const newCanvas = document.getElementById('branchVerticalChart');
        if (!newCanvas) return;
       
        const ctx = newCanvas.getContext('2d');
       
        // Clean up existing chart
        if (charts.branchVerticalChart) {
            charts.branchVerticalChart.destroy();
            charts.branchVerticalChart = null;
        }
       
        const maxValue = Math.max(...mappedData.map(item => item.value));
        const yMax = maxValue * 1.1; // Add 10% padding
       
        charts.branchVerticalChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: mappedData.map(item => item.displayKey),
                datasets: [{
                    label: 'Overdue Ratio (%)',
                    data: mappedData.map(item => item.value),
                    backgroundColor: mappedData.map((item, index) => {
                        const colors = ['#3498DB', '#E74C3C', '#2ECC71', '#F39C12', '#9B59B6'];
                        return colors[index % colors.length];
                    }),
                    borderColor: mappedData.map((item, index) => {
                        const colors = ['#2980B9', '#C0392B', '#27AE60', '#D68910', '#8E44AD'];
                        return colors[index % colors.length];
                    }),
                    borderWidth: 2,
                    borderRadius: 6,
                    datalabels: {
                        color: '#fff',
                        font: { size: 14, weight: 'bold' },
                        anchor: 'end',
                        align: 'top',
                        offset: 8,
                        formatter: function(value) {
                            return value.toFixed(1) + '%';
                        },
                        display: function(context) {
                            return context.dataset.data[context.dataIndex] > 0;
                        }
                    }
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y', // Vertical bars
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        grid: {
                            display: false
                        },
                        ticks: {
                            color: '#1a202c',
                            font: { size: 12, weight: 'bold' }
                        }
                    },
                    y: {
                        beginAtZero: true,
                        max: yMax,
                        grid: {
                            color: 'rgba(0,0,0,0.1)'
                        },
                        ticks: {
                            color: '#1a202c',
                            font: { size: 12 },
                            callback: function(value) {
                                return value.toFixed(1) + '%';
                            }
                        }
                    }
                }
            },
            plugins: [ChartDataLabels]
        });
       
        console.log('Vertical branch chart created successfully');
    } else {
        fallbackDiv.innerHTML = `
            <div style="display: flex; align-items: center; justify-content: center; color: #e53e3e; font-size: 16px; height: 100%; background: #fed7d7; border-radius: 8px; border: 2px dashed #fc8181;">
                <div style="text-align: center;">
                    <div style="font-size: 24px; margin-bottom: 8px;">⚠️</div>
                    <div>No data available</div>
                </div>
            </div>
        `;
    }
}

function createDynamicTreemap(data) {
    console.log('Creating REAL treemap...');
   
    const canvas = document.getElementById('treemapChart');
    const fallbackDiv = document.getElementById('fallback-treemap');
   
    if (!fallbackDiv) {
        console.error('Treemap container not found');
        return;
    }
   
    if (canvas) canvas.style.display = 'none';
    fallbackDiv.style.display = 'block';
   
    // Clear any existing content
    fallbackDiv.innerHTML = '';
   
    if (data && data.length > 0) {
        // Filter out specific branch and get top 5, add more branches if needed
        const excludedBranch = '8125'; // Branch to exclude
       
        // Step 1: Filter out the unwanted branch
        const filteredData = data.filter(item => String(item.key) !== excludedBranch);
        console.log(`Filtered out branch: ${excludedBranch}`);
        console.log(`Available branches after filtering:`, filteredData.length);
       
        // Step 2: Sort by value (overdue ratio) in descending order to get true top 5
        const sortedData = [...filteredData].sort((a, b) => b.value - a.value);
        console.log(`Sorted branches by overdue ratio:`, sortedData.map(item => `${item.key} (${item.value.toFixed(2)}%)`));
       
        // Step 3: Get top 5 from sorted data
        let top5Data = sortedData.slice(0, 5);
       
        // Step 4: If we have less than 5 items after filtering, add more branches from the list
        if (top5Data.length < 5) {
            console.log(`Only ${top5Data.length} branches available, adding more...`);
            const remainingBranches = sortedData.slice(5, 10);
            top5Data = [...top5Data, ...remainingBranches].slice(0, 5);
        }
       
        console.log(`Final Top 5 branches:`, top5Data.map(item => `${item.key} (${item.value.toFixed(2)}%)`));
       
        // Ensure we have exactly 5 items
        top5Data = top5Data.slice(0, 5);
       
        if (top5Data.length === 0) {
            console.error(`No branches available after filtering out ${excludedBranch}`);
            fallbackDiv.innerHTML = `
                <div style="display: flex; align-items: center; justify-content: center; color: #e53e3e; font-size: 16px; height: 100%; background: #fed7d7; border-radius: 8px; border: 2px dashed #fc8181;">
                    <div style="text-align: center;">
                        <div style="font-size: 24px; margin-bottom: 8px;">⚠️</div>
                        <div>No branches available</div>
                        <div style="font-size: 14px; color: #c53030; margin-top: 4px;">All branches may have been filtered out</div>
                    </div>
                </div>
            `;
            return;
        }
       
        // Log the final result for debugging
        console.log(`🎯 Final Top 5 branches:`, top5Data.map(item => `${item.key} (${item.value.toFixed(2)}%)`));
        console.log(`📋 Branch name mapping available:`, branchNameMapping);
        
        // Map branch codes to branch names using branchNameMapping with proper normalization
        const mappedData = top5Data.map(item => {
            // Normalize branch code for mapping lookup (handle both string and numeric codes)
            let normalizedKey = item.key;
            if (typeof normalizedKey === 'string' && normalizedKey.includes('.')) {
                const floatVal = parseFloat(normalizedKey);
                if (!isNaN(floatVal) && floatVal % 1 === 0) {
                    // Remove .0 from integer floats
                    normalizedKey = Math.floor(floatVal).toString();
                }
            } else if (typeof normalizedKey === 'number') {
                normalizedKey = normalizedKey.toString();
            }
            
            // Get branch name from mapping, fallback to original key
            const branchName = branchNameMapping[normalizedKey] || item.key;
            
            // Convert to Title Case (first letter uppercase, rest lowercase)
            const titleCaseName = branchName.toLowerCase().replace(/\b\w/g, l => l.toUpperCase());
            
            return {
                ...item,
                displayKey: titleCaseName, // Use Title Case for display
                originalKey: item.key, // Keep original key for reference
                normalizedKey: normalizedKey // Keep normalized key for debugging
            };
        });
        
        // Log mapping results for debugging
        console.log(`🔄 Branch mapping results:`, mappedData.map(item => 
            `${item.originalKey} → "${item.displayKey}" (normalized: ${item.normalizedKey})`
        ));
       
        // Calculate total to normalize sizes
        const totalValue = mappedData.reduce((sum, item) => sum + item.value, 0);
        const maxValue = Math.max(...mappedData.map(item => item.value || 0));
        const maxProportion = (totalValue > 0) ? (maxValue / totalValue) : 0;
        const baseFontSize = Math.max(10, Math.min(18, 12 + maxProportion * 8));
       
        // Enhanced color palette
        const modernTreemapColors = [
            '#3498DB', '#1976D2', '#2196F3', '#42A5F5', '#64B5F6', '#90CAF9', '#BBDEFB',
            '#00695C', '#00838F', '#26A69A', '#4DB6AC', '#80CBC4', '#B2DFDB', '#E0F2F1',
            '#2E7D32', '#388E3C', '#4CAF50', '#66BB6A', '#81C784', '#A5D6A7', '#C8E6C9',
            '#E65100', '#F57C00', '#FF9800', '#FFB74D', '#FFCC02', '#FFE082', '#FFF3E0'
        ];
       
        // Add modern colors to data with column-based color assignment for 4-column layout
        mappedData.forEach((item, index) => {
            const columnIndex = index % 4;
            const rowIndex = Math.floor(index / 4);
           
            let colorIndex;
            if (columnIndex === 0) {
                colorIndex = rowIndex % 7;
            } else if (columnIndex === 1) {
                colorIndex = 7 + (rowIndex % 7);
            } else if (columnIndex === 2) {
                colorIndex = 14 + (rowIndex % 7);
            } else {
                colorIndex = 21 + (rowIndex % 7);
            }
           
            item.color = modernTreemapColors[colorIndex];
        });
       
        // Enhanced Squarified Treemap algorithm
        function squarifiedTreemap(data, x, y, width, height) {
            const items = [];
            if (!data || data.length === 0 || width <= 0 || height <= 0) return items;

            const total = data.reduce((sum, d) => sum + (d.value || 0), 0) || 1;
            const containerArea = width * height;
            const nodes = [...data]
                .map(d => ({ ...d, area: Math.max(0, (d.value / total) * containerArea) }))
                .sort((a, b) => b.area - a.area);

            let row = [];
            let rectX = x;
            let rectY = y;
            let w = width;
            let h = height;

            function worst(rowAreas, shortSide) {
                if (rowAreas.length === 0) return Infinity;
                const s = rowAreas.reduce((sum, n) => sum + n.area, 0);
                let maxA = 0;
                let minA = Infinity;
                for (const n of rowAreas) {
                    if (n.area > maxA) maxA = n.area;
                    if (n.area < minA) minA = n.area;
                }
                const s2 = s * s;
                const ss2 = shortSide * shortSide;
                return Math.max((ss2 * maxA) / s2, s2 / (ss2 * minA));
            }

            function layoutRow(rowNodes, horizontal) {
                const s = rowNodes.reduce((sum, n) => sum + n.area, 0);
                const gap = 1;
                if (horizontal) {
                    const rowHeight = s / w;
                    let cx = rectX;
                    for (const n of rowNodes) {
                        const cw = n.area / rowHeight;
                        items.push({
                            ...n,
                            x: cx + gap / 2,
                            y: rectY + gap / 2,
                            width: Math.max(0, cw - gap),
                            height: Math.max(0, rowHeight - gap)
                        });
                        cx += cw;
                    }
                    rectY += rowHeight;
                    h -= rowHeight;
                } else {
                    const colWidth = s / h;
                    let cy = rectY;
                    for (const n of rowNodes) {
                        const ch = n.area / colWidth;
                        items.push({
                            ...n,
                            x: rectX + gap / 2,
                            y: cy + gap / 2,
                            width: Math.max(0, colWidth - gap),
                            height: Math.max(0, ch - gap)
                        });
                        cy += ch;
                    }
                    rectX += colWidth;
                    w -= colWidth;
                }
            }

            while (nodes.length > 0) {
                const next = nodes[0];
                if (row.length === 0) {
                    row.push(nodes.shift());
                    continue;
                }
                const horizontal = w >= h;
                const shortSide = horizontal ? h : w;
                const currentWorst = worst(row, shortSide);
                const newWorst = worst([...row, next], shortSide);
                if (currentWorst >= newWorst) {
                    row.push(nodes.shift());
                } else {
                    layoutRow(row, horizontal);
                    row = [];
                }
            }

            if (row.length > 0) {
                layoutRow(row, w >= h);
            }

            return items;
        }

        const containerWidth = fallbackDiv.offsetWidth - 4;
        const containerHeight = fallbackDiv.offsetHeight - 4;
       
        const items = squarifiedTreemap(mappedData, 0, 0, containerWidth, containerHeight);
       
        // Create rectangle elements
        items.forEach((item, index) => {
            const rectDiv = document.createElement('div');
            rectDiv.className = 'treemap-rect';
            rectDiv.setAttribute('role', 'button');
            rectDiv.setAttribute('tabindex', '0');
            rectDiv.setAttribute('aria-label', `${item.displayKey}: ${item.value.toFixed(2)}% overdue ratio`);
           
            const fontSize = baseFontSize;
           
            // Simplified styling for better offline compatibility
            rectDiv.style.position = 'absolute';
            rectDiv.style.left = item.x + 'px';
            rectDiv.style.top = item.y + 'px';
            rectDiv.style.width = item.width + 'px';
            rectDiv.style.height = item.height + 'px';
            rectDiv.style.backgroundColor = item.color; // Use solid color instead of gradient
            rectDiv.style.borderRadius = '8px';
            rectDiv.style.display = 'flex';
            rectDiv.style.flexDirection = 'column';
            rectDiv.style.justifyContent = 'center';
            rectDiv.style.alignItems = 'center';
            rectDiv.style.color = 'white';
            rectDiv.style.fontWeight = 'bold';
            rectDiv.style.fontFamily = 'Arial, sans-serif'; // More compatible font
            rectDiv.style.textAlign = 'center';
            rectDiv.style.cursor = 'pointer';
            rectDiv.style.border = '2px solid rgba(255, 255, 255, 0.5)';
            rectDiv.style.overflow = 'hidden';
            rectDiv.style.zIndex = '1';
            rectDiv.style.animation = `fadeInScale 0.6s ease-out ${index * 0.1}s both`;
           
            // Calculate font size for text-only display (branch names) - reduced by 2 sizes
            const titleFontSize = Math.max(10, Math.min(18, Math.min(item.width, item.height) / 4));
           
            // Simplified content for better offline compatibility - show branch name with dash and percentage
            rectDiv.innerHTML = `
                <div style="font-size: ${titleFontSize}px; font-weight: bold; text-shadow: 1px 1px 2px rgba(0,0,0,0.5); text-align: center; display: flex; align-items: center; justify-content: center; height: 100%;">${item.displayKey} - ${item.value.toFixed(2)}%</div>
            `;
           
            // Simplified hover effects for better offline compatibility
            rectDiv.addEventListener('mouseenter', function() {
                this.style.transform = 'scale(1.05)';
                this.style.border = '2px solid rgba(255, 255, 255, 0.8)';
                this.style.zIndex = '10';
                this.style.opacity = '0.9';
            });
           
            rectDiv.addEventListener('mouseleave', function() {
                this.style.transform = 'scale(1)';
                this.style.border = '2px solid rgba(255, 255, 255, 0.5)';
                this.style.zIndex = '1';
                this.style.opacity = '1';
            });
           
            const handleClick = () => {
                console.log('Clicked on:', item.displayKey, 'Overdue Ratio:', item.value.toFixed(2) + '%', 'Original Code:', item.originalKey, 'Normalized Code:', item.normalizedKey);
                // Add visual feedback for click
                rectDiv.style.transform = 'scale(0.95)';
                setTimeout(() => {
                    rectDiv.style.transform = 'scale(1.08) translateY(-5px) rotate(1deg)';
                }, 150);
            };
           
            rectDiv.addEventListener('click', handleClick);
           
            // Add keyboard navigation
            rectDiv.addEventListener('keydown', function(e) {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    handleClick();
                }
            });
           
            fallbackDiv.appendChild(rectDiv);
        });
       
    } else {
        fallbackDiv.innerHTML = `
            <div style="display: flex; align-items: center; justify-content: center; color: #1a202c; font-size: 16px; height: 100%; background: #f7fafc; border-radius: 8px; border: 2px dashed #cbd5e0;">
                <div style="text-align: center;">
                    <div style="font-size: 24px; margin-bottom: 8px;">📊</div>
                    <div>No branch data available</div>
                    <div style="font-size: 14px; color: #718096; margin-top: 4px;">Try selecting different filters</div>
                </div>
            </div>
        `;
    }
   
    console.log('IMPROVED treemap created with branch name mapping!');
   
    // Add resize handler for responsiveness
    const resizeHandler = () => {
        if (data && data.length > 0) {
            const newContainerWidth = fallbackDiv.offsetWidth - 4;
            const newContainerHeight = fallbackDiv.offsetHeight - 4;
           
            // Only recreate if dimensions changed significantly
            if (Math.abs(newContainerWidth - containerWidth) > 10 || Math.abs(newContainerHeight - containerHeight) > 10) {
                console.log('Treemap container resized, recreating...');
                createDynamicTreemap(data);
            }
        }
    };
   
    // Debounce resize events
    let resizeTimeout;
    window.addEventListener('resize', () => {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(resizeHandler, 250);
    });
   
    // Add simple CSS animations for offline compatibility
    if (!document.getElementById('treemap-animations')) {
        const style = document.createElement('style');
        style.id = 'treemap-animations';
        style.textContent = `
            @keyframes fadeInScale {
                0% {
                    opacity: 0;
                    transform: scale(0.9);
                }
                100% {
                    opacity: 1;
                    transform: scale(1);
                }
            }
        `;
        document.head.appendChild(style);
    }
}

function adjustColor(color, amount) {
    try {
        const usePound = color[0] === '#';
        const col = usePound ? color.slice(1) : color;
       
        // Validate hex color
        if (!/^[0-9A-Fa-f]{6}$/.test(col)) {
            console.warn('Invalid color format:', color);
            return color; // Return original color if invalid
        }
       
        const num = parseInt(col, 16);
        if (isNaN(num)) {
            console.warn('Invalid color number:', col);
            return color;
        }
       
        let r = (num >> 16) + amount;
        let g = (num >> 8 & 0x00FF) + amount;
        let b = (num & 0x0000FF) + amount;
       
        r = Math.max(0, Math.min(255, r));
        g = Math.max(0, Math.min(255, g));
        b = Math.max(0, Math.min(255, b));
       
        const result = (r << 16 | g << 8 | b).toString(16).padStart(6, '0');
        return (usePound ? '#' : '') + result;
    } catch (error) {
        console.warn('Error adjusting color:', error, 'Original color:', color);
        return color; // Return original color on error
    }
}

// Tooltip functions for treemap
function showTreemapTooltip(element, item) {
    hideTreemapTooltip(); // Remove any existing tooltip
   
    const tooltip = document.createElement('div');
    tooltip.id = 'treemap-tooltip';
    tooltip.style.cssText = `
        position: absolute;
        background: rgba(0, 0, 0, 0.9);
        color: white;
        padding: 12px 16px;
        border-radius: 8px;
        font-size: 16px;
        font-weight: 500;
        z-index: 1000;
        pointer-events: none;
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.3);
        border: 1px solid rgba(255, 255, 255, 0.2);
        backdrop-filter: blur(10px);
        animation: tooltipFadeIn 0.2s ease-out;
        max-width: 200px;
    `;
   
    tooltip.innerHTML = `
        <div style="font-weight: 700; margin-bottom: 4px; color: ${item.color};">${item.displayKey}</div>
        <div style="font-size: 14px; opacity: 0.9;">Overdue Ratio: ${item.value.toFixed(2)}%</div>
        <div style="font-size: 14px; opacity: 0.7;">Branch Code: ${item.key}</div>
    `;
   
    document.body.appendChild(tooltip);
   
    // Position tooltip
    const rect = element.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();
   
    let left = rect.left + (rect.width / 2) - (tooltipRect.width / 2);
    let top = rect.top - tooltipRect.height - 10;
   
    // Adjust if tooltip goes off screen
    if (left < 10) left = 10;
    if (left + tooltipRect.width > window.innerWidth - 10) {
        left = window.innerWidth - tooltipRect.width - 10;
    }
    if (top < 10) {
        top = rect.bottom + 10;
    }
   
    tooltip.style.left = left + 'px';
    tooltip.style.top = top + 'px';
}

function hideTreemapTooltip() {
    const tooltip = document.getElementById('treemap-tooltip');
    if (tooltip) {
        tooltip.remove();
    }
}

async function fetchAPI(endpoint) {
    try {
        console.log('Fetching API:', `${API_BASE_URL}${endpoint}`);
        const response = await fetch(`${API_BASE_URL}${endpoint}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`Error fetching ${endpoint}:`, error);
        throw error;
    }
}

function updateStatCardWithComparisons(index, value, unit = '', comparisons = {}) {
    const statCards = document.querySelectorAll('.stat-card');
    if (statCards[index]) {
        const mainValueElement = statCards[index].querySelector('.stat-main-value');
        if (mainValueElement) {
            let displayValue = value;
            if (unit === 'USD') {
                // Round to integer for USD values (no decimal places)
                displayValue = Math.round(Number(value)).toLocaleString('en-US');
            } else if (unit === '%') {
                displayValue = formatNumber(value) + '%';
            } else {
                displayValue = formatNumber(value);
            }
            mainValueElement.textContent = displayValue;
        }
       
        if (comparisons && Object.keys(comparisons).length > 0) {
            updateComparisonTrends(statCards[index], comparisons, unit, index);
        } else {
            const existingTrends = statCards[index].querySelector('.stat-trends');
            if (existingTrends) {
                existingTrends.remove();
            }
        }
    }
}

function updateComparisonTrends(statCard, comparisons, unit, tickerIndex = 0) {
    let trendsContainer = statCard.querySelector('.stat-trends');
    if (!trendsContainer) {
        trendsContainer = document.createElement('div');
        trendsContainer.className = 'stat-trends';
        trendsContainer.style.cssText = `
            display: flex;
            flex-direction: column;
            gap: 8px;
            margin-top: 8px;
            font-size: 16px;
        `;
        statCard.appendChild(trendsContainer);
    }
   
    trendsContainer.innerHTML = '';
   
    const periods = [
        { key: 'last_day', label: 'vs Last Day' },
        { key: 'last_month', label: 'vs Last Month' },
        { key: 'last_year', label: 'vs Last Year' }
    ];
   
    periods.forEach(period => {
        const compData = comparisons[period.key];
        if (compData && compData.change !== undefined) {
            const trendDiv = document.createElement('div');
            trendDiv.className = 'stat-trend-item';
            trendDiv.style.cssText = `
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-size: 16px;
                color: #4a5568;
                padding: 2px 0;
            `;
           
            const change = compData.change || 0;
            const isPositive = change > 0;
            const isNegative = change < 0;
           
            let changeColor = '#1a202c';
            let changeIcon = '';
           
            if (isPositive) {
                // Ticker thứ 3 (Ending Balance): tăng = xanh, giảm = đỏ
                // Ticker 1,2: tăng = đỏ, giảm = xanh
                changeColor = (tickerIndex === 2) ? THEME_COLORS.green : THEME_COLORS.red;
                changeIcon = '▲';
            } else if (isNegative) {
                changeColor = (tickerIndex === 2) ? THEME_COLORS.red : THEME_COLORS.green;
                changeIcon = '▼';
            } else {
                changeIcon = '▪';
            }
           
            let changeText = '';
            if (unit === 'USD') {
                // Round to integer for USD values (no decimal places)
                changeText = Math.round(Math.abs(change)).toLocaleString('en-US');
            } else if (unit === '%') {
                changeText = Math.abs(change).toFixed(2).replace(/\.?0+$/, '') + '%';
            } else {
                changeText = Math.abs(change).toFixed(2).replace(/\.?0+$/, '');
            }
           
            trendDiv.innerHTML = `
                <span style="font-weight: 500;">${period.label}</span>
                <span style="color: ${changeColor}; font-weight: bold; font-size: 15px;">
                    ${changeIcon} ${changeText}
                </span>
            `;
           
            trendsContainer.appendChild(trendDiv);
        }
    });
}

function updateStatCard(index, value, unit = '') {
    updateStatCardWithComparisons(index, value, unit, {});
}

function formatNumber(value) {
    if (typeof value !== 'number') {
        value = parseFloat(value) || 0;
    }
    return value.toFixed(2);
}

// HELPER: Show loading state for charts
function showChartLoading(canvasId, chartName) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    const container = canvas.parentElement;
    if (!container) return;
    container.style.position = container.style.position || 'relative';
    if (document.getElementById(`${canvasId}-loading`)) return;
    const overlay = document.createElement('div');
    overlay.id = `${canvasId}-loading`;
    overlay.style.cssText = `
        position: absolute;
        inset: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        background: rgba(255,255,255,0.6);
        backdrop-filter: blur(1px);
        z-index: 10;
        pointer-events: none;
        color: #4299e1;
        text-align: center;
        font-weight: bold;
    `;
    overlay.innerHTML = `
        <div>
            <div style="font-size: 22px; margin-bottom: 6px;">⏳</div>
            <div style="font-size: 16px;">Loading ${chartName}...</div>
        </div>
    `;
    container.appendChild(overlay);
}

function hideChartLoading(canvasId) {
    const overlay = document.getElementById(`${canvasId}-loading`);
    if (overlay && overlay.parentElement) {
        overlay.parentElement.removeChild(overlay);
    }
}

// ENHANCED: Helper function to show chart errors
function showChartError(canvasId, errorMessage) {
    const canvas = document.getElementById(canvasId);
    if (canvas) {
        const container = canvas.parentElement;
        if (container) {
            container.innerHTML = `
                <div style="display: flex; align-items: center; justify-content: center; height: 300px; color: #e74c3c; text-align: center; padding: 20px;">
                    <div>
                        <div style="font-size: 24px; margin-bottom: 8px;">⚠️</div>
                        <div style="font-size: 16px; font-weight: bold; margin-bottom: 4px;">Chart Error</div>
                        <div style="font-size: 14px; color: #c53030;">${errorMessage}</div>
                    </div>
                </div>
            `;
        }
    }
}