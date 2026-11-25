// Dynamic API URL that works on any machine
const API_BASE_URL = (() => {
    // Check if we're on localhost/127.0.0.1 (development)
    if (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1') {
        return 'http://127.0.0.1:5000/api';
    }
    // Check if we're on the production server itself
    if (window.location.hostname === '<internal-host>') {
        return 'http://<internal-host>:5000/api';
    }
    // For any other hostname, try localhost first, then production
    console.log('🔧 Auto-detecting API URL for hostname:', window.location.hostname);
    return 'http://127.0.0.1:5000/api';
})();

// Log the API URL being used
console.log('🚀 Using API URL:', API_BASE_URL);

// Test API connectivity and fallback if needed
async function testAPIConnectivity() {
    try {
        const testUrl = `${API_BASE_URL}/regions`;
        const response = await fetch(testUrl, { 
            method: 'GET',
            timeout: 3000 // 3 second timeout
        });
        
        if (response.ok) {
            console.log('✅ API connection successful');
            return true;
        } else {
            console.warn('⚠️ API connection failed, status:', response.status);
            return false;
        }
    } catch (error) {
        console.warn('⚠️ API connection failed:', error.message);
        
        // If we're not on localhost and localhost failed, try production
        if (API_BASE_URL.includes('127.0.0.1') && window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
            console.log('🔄 Trying production API as fallback...');
            const productionUrl = 'http://<internal-host>:5000/api';
            try {
                const fallbackResponse = await fetch(`${productionUrl}/regions`, { timeout: 3000 });
                if (fallbackResponse.ok) {
                    console.log('✅ Production API fallback successful');
                    // Update the global API_BASE_URL
                    window.API_BASE_URL = productionUrl;
                    return true;
                }
            } catch (fallbackError) {
                console.error('❌ Production API fallback also failed:', fallbackError.message);
            }
        }
        return false;
    }
}

let selectedDate = null;
let selectedRegion = '';
let selectedBranch = '';
let stackedChart = null;
let monthlyChart = null;
let datePickerInstance = null;

const PIE_COLORS = ['#009688', '#223A5E', '#F4A300', '#2B4C6F'];

// Format currency with proper alignment
function formatCurrency(val) {
    if (!val && val !== 0) return '-';
    return new Intl.NumberFormat('en-US', {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(val);
}

// Format percentage with proper alignment
function formatPercent(val) {
    if (!val && val !== 0) return '-';
    return new Intl.NumberFormat('en-US', {
        style: 'percent',
        minimumFractionDigits: 1,
        maximumFractionDigits: 1
    }).format(val);
}

// Format currency with color
function formatCurrencyWithColor(val, isBold = false) {
    if (!val && val !== 0) return '-';
    const formatted = formatCurrency(Math.abs(val));
    const colorClass = val > 0 ? 'positive' : val < 0 ? 'negative' : '';
    const boldClass = isBold ? 'fw-bold' : '';
    return `<span class="ticker-value ${colorClass} ${boldClass}">${formatted}</span>`;
}

// Show loading state
function setLoading(isLoading) {
    const refreshBtn = document.getElementById('get-data-button');
    if (refreshBtn) {
        refreshBtn.disabled = isLoading;
        refreshBtn.innerHTML = isLoading ? '<i class="fas fa-spinner fa-spin"></i> Loading...' : '<i class="fas fa-sync-alt"></i> Get Data';
    }
}

// Get current date
function getCurrentDate() {
    const input = document.getElementById('date-picker');
    if (input && input.value) return input.value;
    const today = new Date();
    return today.toISOString().slice(0, 10);
}

// Initialize date picker
function initializeDatePicker() {
    const dateInput = document.getElementById('date-picker');
    if (!dateInput) {
        console.error('Date picker input not found');
        return;
    }

    // Destroy existing instance if it exists
    if (datePickerInstance) {
        try {
            datePickerInstance.destroy();
        } catch (e) {
            console.warn('Previous date picker cleanup failed:', e);
        }
    }

    // Initialize flatpickr (same as reference code)
    datePickerInstance = flatpickr(dateInput, {
        dateFormat: "Y-m-d",
        defaultDate: new Date(),
        allowInput: true,
        onChange: function(selectedDates, dateStr, instance) {
            console.log('Date changed:', dateStr);
            selectedDate = dateStr;
            // Update regions list when date changes (to support new region format from 2026-01-16)
            populateRegions();
            // DISABLED: Auto-refresh on date change - User requested manual control only
            // if (window.dateChangeTimeout) {
            //     clearTimeout(window.dateChangeTimeout);
            // }
            // window.dateChangeTimeout = setTimeout(() => {
            //     refreshData();
            // }, 300);
        },
        onReady: function(selectedDates, dateStr, instance) {
            console.log('Date picker ready:', dateStr);
            selectedDate = dateStr;
        }
    });

    // Handle manual input
    dateInput.addEventListener('blur', function() {
        const val = this.value.trim();
        if (/^\d{4}-\d{2}-\d{2}$/.test(val)) {
            try {
                const date = new Date(val);
                if (!isNaN(date.getTime())) {
                    selectedDate = val;
                    if (datePickerInstance) {
                        datePickerInstance.setDate(date, false);
                    }
                    // DISABLED: Auto-refresh on date input - User requested manual control only
                    // refreshData();
                }
            } catch (e) {
                console.error('Invalid date entered:', val);
            }
        }
    });

    selectedDate = getCurrentDate();
    console.log('Initial date set:', selectedDate);
}

// Populate regions
async function populateRegions() {
    try {
        // Get current date to determine which region format to use
        const date = getCurrentDate();
        const response = await fetch(`${API_BASE_URL}/regions?date=${date}`);
        const data = await response.json();
        const select = document.getElementById('region-select');
        if (select && data && !data.error) {
            select.innerHTML = '<option value="">All Regions</option>';
            data.forEach(region => {
                select.innerHTML += `<option value="${region}">${region}</option>`;
            });
        }
    } catch (error) {
        console.error('Error fetching regions:', error);
    }
}

// Clear text helper
function clearText(ids) {
    ids.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = '-';
        }
    });
}

// Clear ticker block
function clearTickerBlock(ids) {
    ids.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = '-';
        }
    });
}

// Format number helper
function formatNumber(value) {
    if (!value || isNaN(value)) return '0';
    const numValue = Number(value);
    if (numValue >= 1000000000) return (numValue / 1000000000).toFixed(1) + 'B';
    if (numValue >= 1000000) return (numValue / 1000000).toFixed(1) + 'M';
    if (numValue >= 1000) return (numValue / 1000).toFixed(1) + 'K';
    return numValue.toLocaleString();
}

// Update tickers
async function updateTickers() {
    try {
        const date = getCurrentDate();
        const region = document.getElementById("region-select")?.value || "";
        const branch = document.getElementById("branch-input")?.value || "";
        
        const url = `${API_BASE_URL}/bang1247-metrics?date=${date}&region=${region}&branch=${branch}`;
        const res = await fetch(url);
        const data = await res.json();
        
        if (data.error) {
            clearText(['current-balance','vs-last-day','vs-last-month','vs-last-year','monthly-avg-balance','monthly-vs-last-day','monthly-vs-last-month','monthly-vs-last-year']);
            return;
        }

        // Update elements safely
        const elements = {
            'current-balance': `<span class="ticker-value"><span>${formatCurrency(data.current_balance)}</span></span>`,
            'vs-last-day': formatCurrencyWithColor(data.vs_last_day),
            'vs-last-month': formatCurrencyWithColor(data.vs_last_month),
            'vs-last-year': formatCurrencyWithColor(data.vs_last_year),
            'monthly-avg-balance': `<span class="ticker-value"><span>${formatCurrency(data.monthly_avg_balance)}</span></span>`,
            'monthly-vs-last-day': formatCurrencyWithColor(data.monthly_vs_last_day),
            'monthly-vs-last-month': formatCurrencyWithColor(data.monthly_vs_last_month),
            'monthly-vs-last-year': formatCurrencyWithColor(data.monthly_vs_last_year)
        };

        Object.keys(elements).forEach(id => {
            const el = document.getElementById(id);
            if (el) el.innerHTML = elements[id];
        });
    } catch (error) {
        console.error('Error updating tickers:', error);
        clearText(['current-balance','vs-last-day','vs-last-month','vs-last-year','monthly-avg-balance','monthly-vs-last-day','monthly-vs-last-month','monthly-vs-last-year']);
    }
}

// Update customer tickers
async function updateCustomerTickers() {
    try {
        const dateStr = getCurrentDate();
        const region = document.getElementById("region-select")?.value || "";
        const branch = document.getElementById("branch-input")?.value || "";

        const res = await fetch(`${API_BASE_URL}/bang3-retail?date=${dateStr}&region=${region}&branch=${branch}`);
        const data = await res.json();
        
        if (data.error) {
            clearText(['retail-customers', 'retail-vs-last-day', 'retail-vs-last-month', 'retail-vs-last-year']);
            return;
        }

        const retail = data.retail_customer || 0;
        
        const elRetailCustomers = document.getElementById('retail-customers');
        if (elRetailCustomers) {
            elRetailCustomers.innerHTML = `<span class="ticker-value"><span>${retail.toLocaleString()}</span></span>`;
        }

        const vsLastDay = data.vs_last_day || 0;
        const vsLastMonth = data.vs_last_month || 0;
        const vsLastYear = data.vs_last_year || 0;

        const elVsLastDay = document.getElementById('retail-vs-last-day');
        if (elVsLastDay) elVsLastDay.innerHTML = formatCurrencyWithColor(vsLastDay);

        const elVsLastMonth = document.getElementById('retail-vs-last-month');
        if (elVsLastMonth) elVsLastMonth.innerHTML = formatCurrencyWithColor(vsLastMonth);

        const elVsLastYear = document.getElementById('retail-vs-last-year');
        if (elVsLastYear) elVsLastYear.innerHTML = formatCurrencyWithColor(vsLastYear);

    } catch (error) {
        console.error('Error fetching retail data:', error);
        clearText(['retail-customers', 'retail-vs-last-day', 'retail-vs-last-month', 'retail-vs-last-year']);
    }
}

// Update stacked bar chart
async function updateStackedBarChart() {
    try {
        const date = getCurrentDate();
        const region = document.getElementById("region-select")?.value || "";
        const branch = document.getElementById("branch-input")?.value || "";

        const url = `${API_BASE_URL}/group-class-stacked-bar?date=${date}&region=${region}&branch=${branch}`;
        console.log('Fetching stacked bar data from:', url);

        const res = await fetch(url);
        const data = await res.json();

        if (data.error) {
            console.error('Error from backend:', data.error);
            return;
        }

        if (!data || !data.labels || !data.datasets || data.datasets.length === 0) {
            console.error('Invalid data format for stacked bar chart');
            return;
        }

        // Convert month labels
        const monthMap = {
            '01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun',
            '07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'
        };
        const engLabels = data.labels.map(m => monthMap[m] || m);

        const ctx = document.getElementById("stacked-bar-chart");
        if (!ctx) {
            console.error('Canvas not found for stacked bar chart');
            return;
        }

        // Destroy existing chart
        if (window.stackedBarChart) {
            window.stackedBarChart.destroy();
        }

        // Color mapping
        // const colorMap = {
        //     '1-CASA 2025': '#00A0E9',
        //     '1-CASA 2024': '#90CAF9',
        //     '2-TIME 2025': '#FF3D7F',
        //     '2-TIME 2024': '#F8BBD0',
        //     '3-ISA 2025': '#8E44AD',
        //     '3-ISA 2024': '#CE93D8'
        // };

        // data.datasets.forEach(ds => {
        //     if (colorMap[ds.label]) ds.backgroundColor = colorMap[ds.label];
        // });

        // Chỉnh datalabels cho ISA
        data.datasets.forEach(ds => {
            if (ds.label && ds.label.includes('ISA')) {
                ds.datalabels = {
                    anchor: 'center',
                    align: 'center',
                    offset: 8,
                    color: '#000',
                    font: { weight: 'bold', size: 11 },
                    formatter: function(value) {
                        return Math.round(value).toLocaleString();
                    },
                    display: function(context) {
                        return context.dataset.data[context.dataIndex] > 0;
                    }
                };
            }
        });

        window.stackedBarChart = new Chart(ctx, {
            type: "bar",
            data: {
                labels: engLabels,
                datasets: data.datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    datalabels: {
                        color: '#000',
                        font: { weight: 'bold', size: 11 },
                        formatter: function(value) {
                            return Math.round(value).toLocaleString();
                        },
                        align: 'center',
                        anchor: 'center',
                        display: function(context) {
                            return context.dataset.data[context.dataIndex] > 0;
                        }
                    }
                },
                scales: {
                    x: {
                        stacked: true,
                        grid: { display: false },
                        ticks: {
                            font: { family: 'Times New Roman, Times, serif', weight: 'bold', size: 16 },
                            color: '#111',
                        },
                    },
                    y: {
                        stacked: true,
                        display: false,
                        beginAtZero: true,
                        max: function(context) {
                            // Calculate max total value (CASA + TDA + ISA) for each month
                            const months = context.chart.data.labels;
                            const datasets = context.chart.data.datasets;
                            let maxTotal = 0;
                            
                            // For each month, calculate total of all account types
                            for (let i = 0; i < months.length; i++) {
                                let monthTotal = 0;
                                // Sum all datasets for this month (CASA + TDA + ISA)
                                datasets.forEach(ds => {
                                    monthTotal += ds.data[i] || 0;
                                });
                                maxTotal = Math.max(maxTotal, monthTotal);
                            }
                            
                            // Set max to total max value + 5% for Monthly Average Balance (MoM) chart (safe for data labels)
                            return maxTotal - (maxTotal * 0.3);
                        }
                    }
                },
                animation: {
                    onComplete: () => renderStackedBarCustomLegend(stackedBarChart)
                }
            },
            plugins: [ChartDataLabels]
        });

        // Custom legend - dynamic years
        const legendDiv = document.getElementById('stacked-bar-custom-legend');
        if (legendDiv) {
            const datasets = data.datasets;
            // Get years from API response or extract from dataset labels
            const prevYear = data.years ? data.years.prev_year : 2024;
            const currentYear = data.years ? data.years.current_year : 2025;
            const yearPrev = datasets.filter(ds => ds.label.includes(String(prevYear)));
            const yearCurrent = datasets.filter(ds => ds.label.includes(String(currentYear)));
            
            legendDiv.innerHTML = `
                <div class="custom-legend-2col" style="display: flex; flex-direction: row; justify-content: center; gap: 32px; margin-top: 4px !important; font-family: 'Times New Roman', Times, serif;">
                    <div class="custom-legend-col">
                        ${yearPrev.map(ds => `
                            <span class="custom-legend-item" style="font-family: 'Times New Roman', Times, serif;">
                                <span class="custom-legend-color" style="background:${ds.backgroundColor}"></span>
                                <span>${ds.label}</span>
                            </span>
                        `).join('')}
                    </div>
                    <div class="custom-legend-col">
                        ${yearCurrent.map(ds => `
                            <span class="custom-legend-item" style="font-family: 'Times New Roman', Times, serif;">
                                <span class="custom-legend-color" style="background:${ds.backgroundColor}"></span>
                                <span>${ds.label}</span>
                            </span>
                        `).join('')}
                    </div>
                </div>
            `;
        }
    } catch (error) {
        console.error('Error updating stacked bar chart:', error);
    }
}

// Update account group table
async function updateAccountGroupTable() {
    try {
        const date = getCurrentDate();
        const region = document.getElementById('region-select')?.value || '';
        const branch = document.getElementById('branch-input')?.value || '';
        const currency = document.getElementById('product-currency-filter')?.value || '';
        const nationality = document.getElementById('product-nationality-filter')?.value || '';
        
        const url = `${API_BASE_URL}/balance-by-product?date=${date}&region=${region}&branch=${branch}&currency=${currency}&nationality=${nationality}`;
        const response = await fetch(url);
        const data = await response.json();
        
        const tbody = document.getElementById("account-group-table-body");
        if (!tbody) return;
        
        tbody.innerHTML = "";

        if (!Array.isArray(data) || data.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6">No data</td></tr>';
            return;
        }

        // Sort the data to move "OTHER" to the bottom of CASA section
        const sortedData = sortBalanceByProductData(data);

        sortedData.forEach(row => {
            const tr = document.createElement("tr");
            const isGroupTotal = ["1-CASA", "2-TIME", "3-ISA"].includes(row["Account Group"]);
            const isGrandTotal = row["Account Group"] === "GRAND TOTAL";
            const isSubItem = !isGroupTotal && !isGrandTotal;

            // Format account group name - capitalize VND and USD
            let displayName = row["Account Group"];
            if (displayName.includes('VND')) {
                displayName = displayName.replace(/VND/gi, 'VND');
            }
            if (displayName.includes('USD')) {
                displayName = displayName.replace(/USD/gi, 'USD');
            }

            tr.innerHTML = `
                <td class="${isSubItem ? 'sub-indent' : ''} ${isGroupTotal ? 'fw-bold' : ''} ${isGrandTotal ? 'bg-light text-primary' : ''} text-start">${displayName}</td>
                <td class="numeric ${isGroupTotal ? 'fw-bold' : ''} ${isGrandTotal ? 'bg-light text-primary' : ''} text-end">${formatCurrency(row["Ending Balance"])}</td>
                <td class="numeric ${isGroupTotal ? 'fw-bold' : ''} ${isGrandTotal ? 'bg-light text-primary' : ''} text-end">${formatPercent(row["Portion"])}</td>
                <td class="numeric ${isGroupTotal ? 'fw-bold' : ''} ${isGrandTotal ? 'bg-light text-primary' : ''} text-end">${formatCurrencyWithColor(row["+- Last Day"] || 0)}</td>
                <td class="numeric ${isGroupTotal ? 'fw-bold' : ''} ${isGrandTotal ? 'bg-light text-primary' : ''} text-end">${formatCurrencyWithColor(row["+- Last Month"] || 0)}</td>
                <td class="numeric ${isGroupTotal ? 'fw-bold' : ''} ${isGrandTotal ? 'bg-light text-primary' : ''} text-end">${formatCurrencyWithColor(row["+- Last Year"] || 0)}</td>
            `;

            if (isGroupTotal) tr.classList.add("group-total");
            if (isGrandTotal) tr.classList.add("grand-total");
            tbody.appendChild(tr);
        });
    } catch (error) {
        console.error("Error loading account group table:", error);
    }
}

// Sort balance by product data to move "OTHER" to bottom of each product group
function sortBalanceByProductData(data) {
    if (!Array.isArray(data)) return data;
    
    console.log('🔄 Sorting balance by product data:', data.map(row => row["Account Group"]));
    
    // Find the indices of main category headers
    const casaIndex = data.findIndex(row => row["Account Group"] === "1-CASA");
    const timeIndex = data.findIndex(row => row["Account Group"] === "2-TIME");
    const isaIndex = data.findIndex(row => row["Account Group"] === "3-ISA");
    
    console.log('📍 Main category indices:', { casaIndex, timeIndex, isaIndex });
    
    if (casaIndex === -1 || timeIndex === -1 || isaIndex === -1) {
        console.log('⚠️ Missing main categories, returning original data');
        return data; // Can't sort if main categories are missing
    }
    
    // Create a copy of the data to sort
    const sortedData = [...data];
    
    // Function to move "OTHER" to bottom of a section
    const moveOtherToBottom = (startIndex, endIndex) => {
        const subcategories = [];
        for (let i = startIndex + 1; i < endIndex; i++) {
            if (data[i] && data[i]["Account Group"] !== data[startIndex]["Account Group"]) {
                subcategories.push({ index: i, row: data[i] });
            }
        }
        
        // Find "OTHER" subcategory in this section
        const otherIndex = subcategories.findIndex(item => 
            item.row["Account Group"].toLowerCase().includes('other')
        );
        
        if (otherIndex !== -1) {
            const otherItem = subcategories[otherIndex];
            console.log(`📝 Moving "OTHER" from position ${otherItem.index} to bottom of section`);
            
            // Remove "OTHER" from its current position
            sortedData.splice(otherItem.index, 1);
            
            // Insert "OTHER" at the bottom of this section (just before next main category)
            sortedData.splice(endIndex - 1, 0, otherItem.row);
        }
    };
    
    // Move "OTHER" to bottom of CASA section
    moveOtherToBottom(casaIndex, timeIndex);
    
    // Move "OTHER" to bottom of TIME section
    moveOtherToBottom(timeIndex, isaIndex);
    
    // Move "OTHER" to bottom of ISA section (before GRAND TOTAL)
    const grandTotalIndex = data.findIndex(row => row["Account Group"] === "GRAND TOTAL");
    if (grandTotalIndex !== -1) {
        moveOtherToBottom(isaIndex, grandTotalIndex);
    } else {
        moveOtherToBottom(isaIndex, data.length);
    }
    
    console.log('📊 Final sorted data:', sortedData.map(row => row["Account Group"]));
    return sortedData;
}

// Draw region pie chart
async function drawRegionPieChart(data) {
    try {
        const canvas = document.getElementById('region-pie-chart');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        if (window.regionPieChart) {
            window.regionPieChart.destroy();
        }
        
        // If no data provided, try to fetch it
        if (!data) {
            const date = getCurrentDate();
            const region = document.getElementById('region-select')?.value || '';
            const branch = document.getElementById('branch-input')?.value || '';
            const product = document.getElementById('region-pie-product-filter')?.value || 'ALL';
            
            try {
                const regionPieRes = await fetch(`${API_BASE_URL}/balance-by-region?date=${date}&region=${region}&branch=${branch}&product=${product}`);
                data = await regionPieRes.json();
                if (data.error) {
                    console.warn('No region pie data available');
                    return;
                }
            } catch (error) {
                console.warn('Could not fetch region pie data:', error);
                return;
            }
        }
        
        // Sort data theo thứ tự chuẩn - Updated to include all regions
        const regionOrder = ['South 1', 'South 2', 'South 3', 'North 1', 'North 2', 'North', 'Others'];
        const sortedData = [...data].sort((a, b) => {
            const aIndex = regionOrder.indexOf(a.region);
            const bIndex = regionOrder.indexOf(b.region);
            // If not found in order, put at the end
            const aOrder = aIndex === -1 ? 999 : aIndex;
            const bOrder = bIndex === -1 ? 999 : bIndex;
            return aOrder - bOrder;
        });
        
        // Hàm format số
        const formatBalance = (value) => {
            if (!value || isNaN(value)) return '0';
            const numValue = Number(value);
            if (numValue >= 1e9) return (numValue / 1e9).toFixed(1) + 'B';
            if (numValue >= 1e6) return (numValue / 1e6).toFixed(1) + 'M';
            return numValue.toLocaleString();
        };
        
        // Map colors: Use color from backend if available, otherwise use fallback colors
        const regionColorMap = {
            'South 1': '#223A5E',   // Dark blue - same as ending balance 30 days dark color
            'South 2': '#1A237E',   // Darker TDA color (darker than #009688)
            'South 3': '#80CBC4',   // Light teal (from backend)
            'North 1': '#FFE082',   // Light yellow (from backend)
            'North 2': '#F4A300',   // Dark yellow/orange (from backend)
            'North': '#FFE082',     // Light yellow (for old data compatibility)
            'Others': '#F47B20'     // Orange (from backend)
        };
        
        window.regionPieChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: sortedData.map(d => d.region),
                datasets: [{
                    data: sortedData.map(d => d.amount),
                    backgroundColor: sortedData.map(d => {
                        // Use color from backend response if available, otherwise use color map
                        return d.color || regionColorMap[d.region] || '#9E9E9E';
                    }),
                    borderWidth: 2,
                    borderColor: '#fff'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: { top: 30, right: 30, bottom: 30, left: 30 }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.raw;
                                const region = context.label;
                                const portion = sortedData.find(d => d.region === region)?.portion || 0;
                                return [
                                    `${region}`,
                                    `Balance: ${formatBalance(value)}`,
                                    `Portion: ${(portion * 100).toFixed(1)}%`
                                ];
                            }
                        },
                        titleFont: { size: 16, weight: 'bold' },
                        bodyFont: { size: 14 },
                        padding: 12,
                        backgroundColor: 'rgba(255,255,255,0.9)',
                        titleColor: '#000',
                        bodyColor: '#666',
                        borderColor: '#ddd',
                        borderWidth: 1
                    },
                    datalabels: {
                        labels: {
                            inside: {
                                color: '#fff', // White for region name and value
                                font: { family: 'Times New Roman, Times, serif', weight: 'bold', size: 16 },
                                anchor: 'center', align: 'center', offset: 0,
                                formatter: function(value, context) {
                                    const region = context.chart.data.labels[context.dataIndex];
                                    const amountStr = Math.round(value).toLocaleString();
                                    return region + '\n' + amountStr;
                                },
                                display: true
                            },
                            outside: {
                                color: '#000', // Black for percentage
                                font: { family: 'Times New Roman, Times, serif', weight: 'bold', size: 16 },
                                anchor: 'end', align: 'end', offset: 10,
                                formatter: function(value, context) {
                                    const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                    const portion = total ? (value / total) * 100 : 0;
                                    return portion.toFixed(0) + '%';
                                },
                                leaderLine: { display: true, color: '#000', width: 1 },
                                display: true
                            }
                        }
                    }
                }
            },
            plugins: [ChartDataLabels]
        });
    } catch (error) {
        console.error('Error drawing region pie chart:', error);
        // Clear chart nếu lỗi
        const canvas = document.getElementById('region-pie-chart');
        if (canvas && window.regionPieChart) {
            window.regionPieChart.destroy();
            window.regionPieChart = null;
        }
    }
}

// Draw term mix chart
async function drawTermMixChart(date, region, branch) {
    try {
        const res = await fetch(`${API_BASE_URL}/balance-by-term?date=${date}&region=${region}&branch=${branch}`);
        const data = await res.json();
        if (data.error) return;
        const canvas = document.getElementById('term-mix-chart');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        if (window.termChart) window.termChart.destroy();
        // Label mapping và order chuẩn
        const labelMapping = {
            'NO_TENNOR': 'No Tennor',
            'UNDER 1M': '< 1M',
            'M1_UNDER 6M': '1M-6M',
            'M6_TO_12M': '6M-12M',
            'ABOVE 12M TO 24M': '12M-24M',
            'ABOVE 24M': '>24M'
        };
        const order = ['NO_TENNOR', 'UNDER 1M', 'M1_UNDER 6M', 'M6_TO_12M', 'ABOVE 12M TO 24M', 'ABOVE 24M'];
        const sortedData = [...data].sort((a, b) => order.indexOf(a.tennor) - order.indexOf(b.tennor));
        // Format số
        const formatBalance = (value) => {
            if (!value || isNaN(value)) return '';
            const numValue = Number(value);
            if (numValue >= 1e9) return (numValue / 1e9).toFixed(1) + 'B';
            if (numValue >= 1e6) return (numValue / 1e6).toFixed(1) + 'M';
            return Math.round(numValue).toLocaleString();
        };
        // Lấy tháng từ date và cập nhật tiêu đề
        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        let basedate = date;
        let monthLabel = '';
        if (basedate && basedate.length >= 7) {
            const m = parseInt(basedate.slice(5, 7), 10);
            if (m >= 1 && m <= 12) monthLabel = monthNames[m - 1];
        }
        // Cập nhật tiêu đề bảng và chart
        const tableHeader = document.querySelector('.table-header h3');
        if (tableHeader && tableHeader.textContent.includes('Deposit by Term and IR')) {
            tableHeader.textContent = monthLabel ? `Deposit by Term and IR (in ${monthLabel})` : 'Deposit by Term and IR (in Month)';
        }
        const chartSection = document.querySelectorAll('.table-container .table-header h3');
        chartSection.forEach(h3 => {
            if (h3.textContent.includes('Deposit by Term and IR')) {
                h3.textContent = monthLabel ? `Deposit by Term and IR (in ${monthLabel})` : 'Deposit by Term and IR (in Month)';
            }
        });
        window.termChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: sortedData.map(d => labelMapping[d.tennor] || d.tennor),
                datasets: [
                    {
                        type: 'bar',
                        label: 'Balance',
                        data: sortedData.map(d => d.balance),
                        backgroundColor: '#009688', // TDA 2025 color
                        order: 2,
                        yAxisID: 'y'
                    },
                    {
                        type: 'line',
                        label: 'Avg Interest',
                        data: sortedData.map(d => d.avg_intr),
                        borderColor: '#F4A300', // CASA 2025 color
                        backgroundColor: '#F4A300', // CASA 2025 color
                        borderWidth: 3,
                        pointRadius: 4,
                        pointHoverRadius: 6,
                        pointBackgroundColor: '#F4A300', // CASA 2025 color
                        pointBorderColor: '#fff',
                        pointBorderWidth: 3,
                        order: 1,
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: { top: 40, bottom: 30 }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.raw;
                                if (context.dataset.label === 'Balance') {
                                    return `${context.dataset.label}: ${formatBalance(value)}`;
                                }
                                return `${context.dataset.label}: ${value ? value.toFixed(2) : ''}%`;
                            }
                        }
                    },
                    datalabels: {
                        display: true,
                        color: function(context) {
                            // Bar: black, Line: orange
                            return context.dataset.type === 'line' ? '#F47B20' : '#000';
                        },
                        font: function(context) {
                            return {
                                family: 'Times New Roman, Times, serif',
                                weight: 'bold',
                                size: context.dataset.type === 'bar' ? 14 : 20 // Bar: 14, Line: 20
                            };
                        },
                        formatter: function(value, context) {
                            if (context.dataset.type === 'bar') {
                                if (!value || isNaN(value)) return '';
                                if (value >= 1e9) return (value / 1e9).toFixed(1) + 'B';
                                if (value >= 1e6) return (value / 1e6).toFixed(1) + 'M';
                                return Math.round(value).toLocaleString();
                            } else {
                                return value ? value.toFixed(2) + '%' : '';
                            }
                        },
                        align: function(context) {
                            return context.dataset.type === 'bar' ? 'end' : 'top';
                        },
                        anchor: function(context) {
                            return context.dataset.type === 'bar' ? 'end' : 'center';
                        },
                        offset: function(context) {
                            return context.dataset.type === 'bar' ? 0 : 0;
                        },
                        clamp: false
                    }
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: {
                            font: { family: 'Times New Roman, Times, serif', weight: 'bold', size: 16 },
                            color: '#000',
                            maxRotation: 45,
                            callback: function(value, index) {
                                return this.getLabelForValue ? this.getLabelForValue(value) : value;
                            }
                        }
                    },
                    y: {
                        display: false,
                        stacked: true,
                        beginAtZero: true,
                        barPercentage: 1.0,
                        categoryPercentage: 0.9,
                        title: { display: false },
                        ticks: {
                            font: { weight: 'bold', size: 12 },
                            callback: function(value) {
                                return value >= 1e12 ? (value / 1e12) + 'T' : value.toLocaleString();
                            }
                        },
                        grid: { color: 'rgba(0, 0, 0, 0.1)' }
                    },
                    y1: {
                        display: false,
                        min: 0,
                        max: 6.5
                    }
                }
            },
            plugins: [ChartDataLabels]
        });
    } catch (error) {
        console.error('Error drawing term mix chart:', error);
        const canvas = document.getElementById('term-mix-chart');
        if (canvas && window.termChart) {
            window.termChart.destroy();
            window.termChart = null;
        }
    }
}

// Simple gauge rendering function - FIXED VERSION
function renderSimpleYoYGauge(canvasId, yoyValue, targetValue, color) {
    console.log(`🎨 Rendering gauge ${canvasId}:`, { yoyValue, targetValue, color });
    const canvas = document.getElementById(canvasId);
    if (!canvas) {
        console.error(`❌ Canvas ${canvasId} not found`);
        return;
    }
    // Ensure canvas has proper dimensions
    canvas.width = 280;
    canvas.height = 200;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    // Calculate gauge parameters
    const centerX = canvas.width / 2;
    const centerY = canvas.height / 2 + 20;
    const radius = 80;
    const valueK = yoyValue; // đã là K
    const targetK = targetValue >= 1e6 ? targetValue / 1e3 : targetValue; // chuyển M về K nếu cần
    const percent = targetK > 0 ? Math.max(0, Math.abs(valueK) / targetK) : 0;
    const percentage = percent * 100;
    // Draw background arc
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius, Math.PI, 0, false);
    ctx.lineWidth = 20;
    ctx.strokeStyle = '#e5e7eb';
    ctx.stroke();
    // Draw progress arc
    if (percent > 0) {
        ctx.beginPath();
        ctx.arc(centerX, centerY, radius, Math.PI, Math.PI + Math.PI * percent, false);
        ctx.lineWidth = 20;
        ctx.strokeStyle = color;
        ctx.stroke();
    }
    // Format display value
    let displayValue = '';
    if (Math.abs(yoyValue) >= 1e9) {
        displayValue = (yoyValue/1e9).toFixed(1) + 'B';
    } else if (Math.abs(yoyValue) >= 1e6) {
        displayValue = (yoyValue/1e6).toFixed(1) + 'M';
    } else if (Math.abs(yoyValue) >= 1e3) {
        displayValue = (yoyValue/1e3).toFixed(0) + 'K';
    } else {
        displayValue = Math.round(yoyValue).toString();
    }
    // Draw center value
    ctx.font = 'bold 28px Arial';
    ctx.fillStyle = color;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(displayValue, centerX, centerY);
    // KHÔNG vẽ % ở giữa gauge nữa!
    // Draw scale labels
    ctx.font = '12px Arial';
    ctx.fillStyle = '#333';
    ctx.textAlign = 'left';
    ctx.fillText('0', centerX - radius - 15, centerY + 30);
    ctx.textAlign = 'right';
    let targetLabel = '';
    if (targetValue >= 1e9) {
        targetLabel = (targetValue/1e9).toFixed(0) + 'B';
    } else if (targetValue >= 1e6) {
        targetLabel = (targetValue/1e6).toFixed(0) + 'M';
    } else if (targetValue >= 1e3) {
        targetLabel = (targetValue/1e3).toFixed(0) + 'K';
    } else {
        targetLabel = targetValue.toString();
    }
    ctx.fillText(targetLabel, centerX + radius + 15, centerY + 30);
    console.log(`✅ Gauge ${canvasId} rendered successfully: ${displayValue} (${percentage.toFixed(1)}%)`);
}

// Update gauge display function
function updateGaugeDisplay(gaugeId, value, target) {
    const valueElement = document.getElementById(`${gaugeId}-value`);
    const targetElement = document.getElementById(`${gaugeId}-target`);
    if (valueElement) {
        // Tính phần trăm giống hệt logic trong renderSimpleYoYGauge
        let targetK = target >= 1e6 ? target / 1e3 : target;
        let percent = targetK > 0 ? Math.max(0, Math.abs(value) / targetK) : 0;
        let percentage = (percent * 100).toFixed(1) + '%';
        let percentColor = percent >= 1 ? '#10b981' : '#ef4444';
        // Hiển thị % to, nhích lên gần gauge hơn
        valueElement.innerHTML = `<div style=\"font-size:28px;font-weight:bold;line-height:1.1;text-align:center;color:${percentColor};margin-top:4px;\">${percentage}</div>`;
    }
    if (targetElement) {
        let targetDisplay;
        if (target >= 1e9) {
            targetDisplay = (target/1e9).toFixed(0) + 'B';
        } else if (target >= 1e6) {
            targetDisplay = (target/1e6).toFixed(0) + 'M';
        } else if (target >= 1e3) {
            targetDisplay = (target/1e3).toFixed(0) + 'K';
        } else {
            targetDisplay = target.toString();
        }
        targetElement.textContent = targetDisplay;
    }
}

// FIXED: Main function to update all gauges
async function updateAllGauges() {
    const date = getCurrentDate();
    const region = document.getElementById('region-select')?.value || '';
    const branch = document.getElementById('branch-input')?.value || '';
    console.log('🔄 Starting gauge update...', { date, region, branch });
    try {
        // Show loading state on all gauges
        ['gauge-all', 'gauge-casa', 'gauge-tda'].forEach(id => {
            const canvas = document.getElementById(id);
            if (canvas) {
                const ctx = canvas.getContext('2d');
                ctx.clearRect(0, 0, canvas.width || 280, canvas.height || 200);
                ctx.font = '16px Arial';
                ctx.fillStyle = '#999';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText('Loading...', (canvas.width || 280)/2, (canvas.height || 200)/2);
            }
        });
        console.log('📡 Fetching gauge data...');
        // Fetch data for all 3 gauges with error handling
        const responses = await Promise.allSettled([
            fetch(`${API_BASE_URL}/gauge-all?date=${date}&region=${region}&branch=${branch}`),
            fetch(`${API_BASE_URL}/gauge-data?date=${date}&region=${region}&branch=${branch}`),
            fetch(`${API_BASE_URL}/gauge-metrics?date=${date}&region=${region}&branch=${branch}`)
        ]);
        // Process responses
        const allData = responses[0].status === 'fulfilled' ? await responses[0].value.json() : null;
        const casaData = responses[1].status === 'fulfilled' ? await responses[1].value.json() : null;
        const tdaData = responses[2].status === 'fulfilled' ? await responses[2].value.json() : null;
        console.log('📊 API Responses:', {
            all: allData,
            casa: casaData,
            tda: tdaData
        });
        // Render gauges with fallback values
        const allValue = allData?.all_balance || 0;
        const allTargetOld = allData?.target || 300000000; // Keep old format for later use
        const casaValue = casaData?.casa_balance || 0;
        const casaTargetOld = casaData?.target || 100000000; // Keep old format for later use
        const tdaValue = tdaData?.tda_growth || 0;
        const tdaTargetOld = tdaData?.target || 100000000; // Keep old format for later use
        
        // Update target to current value (keep old format for later use)
        const allTarget = Math.abs(allValue) || allTargetOld;
        const casaTarget = Math.abs(casaValue) || casaTargetOld;
        const tdaTarget = Math.abs(tdaValue) || tdaTargetOld;
        
        console.log('🎯 Rendering gauges with values:', {
            all: { value: allValue, target: allTarget, oldTarget: allTargetOld },
            casa: { value: casaValue, target: casaTarget, oldTarget: casaTargetOld },
            tda: { value: tdaValue, target: tdaTarget, oldTarget: tdaTargetOld }
        });
        // Render each gauge
        renderSimpleYoYGauge('gauge-all', allValue, allTarget, '#223A5E'); // ISA 2025
        renderSimpleYoYGauge('gauge-casa', casaValue, casaTarget, '#009688'); // TDA 2025
        renderSimpleYoYGauge('gauge-tda', tdaValue, tdaTarget, '#F4A300'); // CASA 2025
        // Update display text
        updateGaugeDisplay('gauge-all', allValue, allTarget);
        updateGaugeDisplay('gauge-casa', casaValue, casaTarget);
        updateGaugeDisplay('gauge-tda', tdaValue, tdaTarget);
        console.log('✅ All gauges updated successfully');
    } catch (error) {
        console.error('❌ Error updating gauges:', error);
        // Show error state
        ['gauge-all', 'gauge-casa', 'gauge-tda'].forEach(id => {
            const canvas = document.getElementById(id);
            if (canvas) {
                const ctx = canvas.getContext('2d');
                ctx.clearRect(0, 0, canvas.width || 280, canvas.height || 200);
                ctx.font = '16px Arial';
                ctx.fillStyle = '#ef4444';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText('Error', (canvas.width || 280)/2, (canvas.height || 200)/2);
            }
        });
        // Set default displays
        updateGaugeDisplay('gauge-all', 0, 300000000);
        updateGaugeDisplay('gauge-casa', 0, 100000000);
        updateGaugeDisplay('gauge-tda', 0, 100000000);
    }
}

// Test functions for debugging
function testGaugeRendering() {
    console.log('🧪 Testing gauge rendering manually...');
    renderSimpleYoYGauge('gauge-all', 150000000, 300000000, '#22AFFF');
    renderSimpleYoYGauge('gauge-casa', 40000000, 100000000, '#10B981');
    renderSimpleYoYGauge('gauge-tda', 110000000, 100000000, '#F47B20');
    updateGaugeDisplay('gauge-all', 150000000, 300000000);
    updateGaugeDisplay('gauge-casa', 40000000, 100000000);
    updateGaugeDisplay('gauge-tda', 110000000, 100000000);
}

async function testGaugeAPIs() {
    const date = getCurrentDate();
    console.log('🧪 Testing gauge APIs with date:', date);
    try {
        const [allRes, casaRes, tdaRes] = await Promise.all([
            fetch(`${API_BASE_URL}/gauge-all?date=${date}`),
            fetch(`${API_BASE_URL}/gauge-data?date=${date}`),
            fetch(`${API_BASE_URL}/gauge-metrics?date=${date}`)
        ]);
        const allData = await allRes.json();
        const casaData = await casaRes.json();
        const tdaData = await tdaRes.json();
        console.log('🔍 API Test Results:', {
            all: allData,
            casa: casaData,
            tda: tdaData
        });
        return { allData, casaData, tdaData };
    } catch (error) {
        console.error('❌ API Test Failed:', error);
    }
}

// Export functions for global access
window.testGaugeRendering = testGaugeRendering;
window.testGaugeAPIs = testGaugeAPIs;
window.updateAllGauges = updateAllGauges;
window.renderSimpleYoYGauge = renderSimpleYoYGauge;

// Initialize gauges when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Initializing gauges...');
    // Wait a bit for other components to load
    setTimeout(() => {
        updateAllGauges();
    }, 1000);
});

// Also call from refreshData function
if (typeof refreshData === 'function') {
    const originalRefreshData = refreshData;
    refreshData = async function() {
        await originalRefreshData();
        setTimeout(updateAllGauges, 500);
    };
}

// Main refresh function
async function refreshData() {
    try {
        setLoading(true);
       
        const date = getCurrentDate();
        const region = document.getElementById("region-select")?.value || "";
        const branch = document.getElementById("branch-input")?.value || "";

        if (!date) {
            console.error('No date selected');
            return;
        }

        console.log('Refreshing data with:', { date, region, branch });

        // Update all components
        await Promise.all([
            updateTickers(),
            updateCustomerTickers(),
            updateStackedBarChart(),
            updateAccountGroupTable()
        ]);
        
        // Update gauges
        await updateAllGauges();
       
        // Fetch and draw region pie chart
        try {
            const product = document.getElementById('region-pie-product-filter')?.value || 'ALL';
            const regionPieRes = await fetch(`${API_BASE_URL}/balance-by-region?date=${date}&region=${region}&branch=${branch}&product=${product}`);
            const regionPieData = await regionPieRes.json();
            if (!regionPieData.error) {
                await drawRegionPieChart(regionPieData);
            }
        } catch (error) {
            console.error('Error with region pie chart:', error);
        }
        
        // Draw other charts with delays
        setTimeout(() => drawTermMixChart(date, region, branch), 100);

        setTimeout(() => drawKStudyNewOpenChart(), 300);
        setTimeout(() => drawCasaCustomerChart(), 350);
        setTimeout(() => drawDailyBalanceChart(), 400);
        
    } catch (error) {
        console.error('Error refreshing data:', error);
    } finally {
        setLoading(false);
    }
}

// Event listeners setup
function setupEventListeners() {
    // Get data button
    const getDataBtn = document.getElementById('get-data-button');
    if (getDataBtn) {
        getDataBtn.addEventListener('click', refreshData);
    }
    
    // Region select
    const regionSelect = document.getElementById('region-select');
    if (regionSelect) {
        regionSelect.addEventListener('change', () => {
            // DISABLED: Auto-refresh on region change - User requested manual control only
            // setTimeout(refreshData, 100);
        });
    }
    
    // Branch input
    const branchInput = document.getElementById('branch-input');
    if (branchInput) {
        branchInput.addEventListener('blur', () => {
            // DISABLED: Auto-refresh on branch input - User requested manual control only
            // setTimeout(refreshData, 100);
        });
    }
    
    // Product filter for daily balance chart
    const productFilter = document.getElementById('product-filter');
    if (productFilter) {
        productFilter.addEventListener('change', drawDailyBalanceChart);
    }
    
    // Product filter for region pie chart
    const regionPieProductFilter = document.getElementById('region-pie-product-filter');
    if (regionPieProductFilter) {
        regionPieProductFilter.addEventListener('change', async () => {
            await drawRegionPieChart();
        });
    }
}

// Navigation tabs setup
function initializeNavigationTabs() {
    document.querySelectorAll('.nav-tab').forEach(tab => {
        tab.addEventListener('click', function() {
            const targetPage = this.getAttribute('data-page');
            
            // Update active tab
            document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
            this.classList.add('active');
            
            // Show target page
            document.querySelectorAll('.page-content').forEach(page => {
                page.classList.remove('active');
            });
            const targetPageElement = document.getElementById(targetPage + '-page');
            if (targetPageElement) {
                targetPageElement.classList.add('active');
            }
            
            // Resize charts after tab switch
            setTimeout(() => {
                Object.values(Chart.instances || {}).forEach(chart => {
                    if (chart && typeof chart.resize === 'function') {
                        try {
                            chart.resize();
                        } catch (e) {
                            console.warn('Failed to resize chart:', e);
                        }
                    }
                });
                
                        // Redraw specific charts when switching tabs
        if (targetPage === 'insights') {
            console.log('🔄 Switching to insights page...');
            // Charts will be redrawn when needed
        } else if (targetPage === 'overview') {
            console.log('🔄 Switching to overview page...');
            // Redraw overview charts if they exist
            setTimeout(() => {
                if (window.regionPieChart && window.regionPieChart.data) {
                    console.log('🔄 Redrawing region pie chart...');
                    drawRegionPieChart();
                }
            }, 100);
        }
            }, 100);
        });
    });
}

// Product filters setup
// Function to convert text to title case (first letter uppercase)
// Special handling for VND and USD to keep them uppercase
function toTitleCase(str) {
    if (!str) return '';
    
    // Special case for VND and USD - keep them uppercase
    if (str.toUpperCase() === 'VND' || str.toUpperCase() === 'USD') {
        return str.toUpperCase();
    }
    
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}

async function populateProductTableFilters() {
    try {
        // Currencies
        const currencyResponse = await fetch(`${API_BASE_URL}/currencies`);
        const currencies = await currencyResponse.json();
        
        const currencySelect = document.getElementById('product-currency-filter');
        if (currencySelect && Array.isArray(currencies)) {
            currencySelect.innerHTML = '<option value="">All Currencies</option>';
            
            // Sort currencies to move "Other" to the bottom
            const sortedCurrencies = [...currencies].sort((a, b) => {
                const aLower = a.toLowerCase();
                const bLower = b.toLowerCase();
                
                // "Other" should always be at the bottom
                if (aLower.includes('other')) return 1;
                if (bLower.includes('other')) return -1;
                
                // Sort alphabetically for other currencies
                return aLower.localeCompare(bLower);
            });
            
            sortedCurrencies.forEach(currency => {
                const option = document.createElement('option');
                option.value = currency;
                option.textContent = toTitleCase(currency);
                currencySelect.appendChild(option);
            });
        }

        // Nationalities
        const nationalityResponse = await fetch(`${API_BASE_URL}/nationalities`);
        const nationalities = await nationalityResponse.json();
        
        const nationalitySelect = document.getElementById('product-nationality-filter');
        if (nationalitySelect && Array.isArray(nationalities)) {
            nationalitySelect.innerHTML = '<option value="">All Nationalities</option>';
            
            // Sort nationalities to move "Other" to the bottom
            const sortedNationalities = [...nationalities].sort((a, b) => {
                const aLower = a.toLowerCase();
                const bLower = b.toLowerCase();
                
                // "Other" should always be at the bottom
                if (aLower.includes('other')) return 1;
                if (bLower.includes('other')) return -1;
                
                // Sort alphabetically for other nationalities
                return aLower.localeCompare(bLower);
            });
            
            sortedNationalities.forEach(nationality => {
                const option = document.createElement('option');
                option.value = nationality;
                option.textContent = toTitleCase(nationality);
                nationalitySelect.appendChild(option);
            });
        }
    } catch (error) {
        console.error('Error populating product table filters:', error);
    }
}

function initializeProductTableFilters() {
    populateProductTableFilters();
    
    const currencyFilter = document.getElementById('product-currency-filter');
    const nationalityFilter = document.getElementById('product-nationality-filter');
    const clearFiltersBtn = document.getElementById('clear-product-filters');
    const getProductDataBtn = document.getElementById('get-product-data-button');
    
    if (currencyFilter) {
        currencyFilter.addEventListener('change', () => {
            // DISABLED: Auto-refresh on currency filter change - User requested manual control only
            // setTimeout(updateAccountGroupTable, 100);
        });
    }
    
    if (nationalityFilter) {
        nationalityFilter.addEventListener('change', () => {
            // DISABLED: Auto-refresh on nationality filter change - User requested manual control only
            // setTimeout(updateAccountGroupTable, 100);
        });
    }
    
    if (getProductDataBtn) {
        getProductDataBtn.addEventListener('click', async () => {
            try {
                // Show loading state
                getProductDataBtn.disabled = true;
                getProductDataBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Loading...';
                
                // Update the product table with current filters
                await updateAccountGroupTable();
                
                console.log('✅ Product table data refreshed successfully');
            } catch (error) {
                console.error('❌ Error refreshing product table data:', error);
            } finally {
                // Reset button state
                getProductDataBtn.disabled = false;
                getProductDataBtn.innerHTML = '<i class="fas fa-sync-alt"></i> Get Data';
            }
        });
    }
    
    if (clearFiltersBtn) {
        clearFiltersBtn.addEventListener('click', () => {
            if (currencyFilter) currencyFilter.value = '';
            if (nationalityFilter) nationalityFilter.value = '';
            // DISABLED: Auto-refresh on clear filters - User requested manual control only
            // setTimeout(updateAccountGroupTable, 100);
        });
    }
}

// Main initialization
async function initializeDashboard() {
    console.log('🚀 Dashboard initializing...');
    
    try {
        // Test API connectivity first
        console.log('🔍 Testing API connectivity...');
        const apiConnected = await testAPIConnectivity();
        
        if (!apiConnected) {
            console.warn('⚠️ API not available, dashboard may not work properly');
        }
        
        // Initialize components
        initializeDatePicker();
        populateRegions();
        initializeNavigationTabs();
        setupEventListeners();
        initializeProductTableFilters();
        
        // ENABLED: Auto-load initial data
        setTimeout(() => {
            refreshData();
            // Force draw K-Study chart after initial load
            setTimeout(() => {
                console.log('🔄 Force drawing K-Study chart after initial load...');
                drawKStudyNewOpenChart();
            }, 1000);
        }, 500);
        
        console.log('ℹ️ Dashboard loaded. Auto-loading data...');
        console.log('📋 Manual Controls:');
        console.log('   - Click "Get Data" button to refresh all data');
        console.log('   - Use Ctrl+Alt+R to reload the page');
        console.log('   - Change filters, then click "Get Data" to apply');
        
        // Show user-friendly message
        const getDataBtn = document.getElementById('get-data-button');
        if (getDataBtn) {
            getDataBtn.style.backgroundColor = '#28a745';
            getDataBtn.innerHTML = '<i class="fas fa-sync-alt"></i> Get Data';
            getDataBtn.title = 'Click to load data manually. Auto-refresh is disabled.';
        }
        
        console.log('✅ Dashboard initialized successfully');
    } catch (error) {
        console.error('❌ Dashboard initialization error:', error);
    }
}

// Global exports for HTML access
window.refreshData = refreshData;
window.getCurrentDate = getCurrentDate;
window.formatCurrency = formatCurrency;
window.formatPercent = formatPercent;
window.formatCurrencyWithColor = formatCurrencyWithColor;
window.updateAccountGroupTable = updateAccountGroupTable;
window.initializeDashboard = initializeDashboard;
window.testAPIConnectivity = testAPIConnectivity;
window.drawCasaCustomerChart = drawCasaCustomerChart;

// Function to manually change API URL (for debugging)
window.setAPIUrl = function(newUrl) {
    window.API_BASE_URL = newUrl;
    console.log('🔧 API URL changed to:', newUrl);
    // Refresh data with new URL
    setTimeout(refreshData, 100);
};

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeDashboard);
} else {
    initializeDashboard();
}

// CASA Customer Donut Chart function (single ring)
async function drawCasaCustomerChart() {
    console.log('🌻 Starting CASA Customer Donut Chart...');
    try {
        const date = getCurrentDate();
        const region = document.getElementById('region-select')?.value || '';
        const branch = document.getElementById('branch-input')?.value || '';
        const chartContainer = document.getElementById('casa-customer-chart');
        if (!chartContainer) {
            console.error('❌ CASA chart container not found');
            return;
        }
        // Clear existing content and set up container
        chartContainer.innerHTML = '';
        chartContainer.style.position = 'relative';
        chartContainer.style.width = '100%';
        chartContainer.style.height = '500px';
        chartContainer.style.display = 'flex';
        chartContainer.style.flexDirection = 'column';
        chartContainer.style.alignItems = 'center';
        chartContainer.style.justifyContent = 'center';
        // Fetch data from API
        const url = `${API_BASE_URL}/casa-customer?date=${date}&region=${region}&branch=${branch}`;
        console.log('🔍 Fetching CASA customer data from:', url);
        const res = await fetch(url);
        const data = await res.json();
        if (data.error || !data) {
            console.error('❌ CASA Donut Chart: No data available');
            chartContainer.innerHTML = '<div style="display: flex; align-items: center; justify-content: center; height: 100%; color: #666; font-size: 16px;">No data available</div>';
            return;
        }
        console.log('📊 CASA Customer data:', data);
        const {
            retail_customer = 0,
            casa_customer = 0,
            casa_active = 0,
            casa_dormant = 0
        } = data;
        // Validate data
        if (retail_customer === 0) {
            console.warn('⚠️ CASA Donut Chart: No retail customers found');
            chartContainer.innerHTML = '<div style="display: flex; align-items: center; justify-content: center; height: 100%; color: #666; font-size: 16px;">No customer data</div>';
            return;
        }
        // Calculate derived values
        const retail_non_casa = Math.max(0, retail_customer - casa_customer);
        // Color scheme - Matching the reference image
        const colors = {
            non_casa: '#223A5E',    // Dark blue for Non-CASA
            active: '#009688',      // Teal for Active
            dormant: '#b0b0b0'      // Light gray for Dormant
        };
        // Create HTML structure with Chart.js
        chartContainer.innerHTML = `
            <div style="position: relative; width: 100%; height: 400px;">
                <canvas id="casa-donut-canvas" style="cursor: pointer;"></canvas>
                <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); text-align: center; pointer-events: none; z-index: 10;">
                    <div style="font-size: 12px; font-weight: bold; color: #007bff; margin-bottom: 3px;">Retail Customer</div>
                    <div style="font-size: 18px; font-weight: bold; color: #007bff;">${retail_customer.toLocaleString()}</div>
                </div>
            </div>
        `;
        
        const canvas = document.getElementById('casa-donut-canvas');
        const ctx = canvas.getContext('2d');
        
        // Prepare data for Chart.js
        const chartData = [];
        const chartLabels = [];
        const chartColors = [];
        
        if (retail_non_casa > 0) {
            chartData.push(retail_non_casa);
            chartLabels.push('Non-casa');
            chartColors.push(colors.non_casa);
        }
        
        if (casa_active > 0) {
            chartData.push(casa_active);
            chartLabels.push('Active');
            chartColors.push(colors.active);
        }
        
        if (casa_dormant > 0) {
            chartData.push(casa_dormant);
            chartLabels.push('Dormant');
            chartColors.push(colors.dormant);
        }
        
        // Create Chart.js doughnut chart
        window.casaCustomerChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: chartLabels,
                datasets: [{
                    data: chartData,
                    backgroundColor: chartColors,
                    borderWidth: 2,
                    borderColor: '#fff'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: { top: 90, right: 90, bottom: 90, left: 90 } // Giảm độ rộng vòng tròn ngoài
                },
                cutout: '50%', // Vòng tròn bên trong to hơn một chút
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.raw;
                                const percentage = ((value / retail_customer) * 100).toFixed(1);
                                return `${context.label}: ${value.toLocaleString()} (${percentage}%)`;
                            }
                        }
                    },
                    datalabels: {
                        labels: {
                            inside: {
                                display: false // Bỏ % trong chart
                            },
                            outside: {
                                color: function(context) {
                                    return context.dataset.backgroundColor[context.dataIndex];
                                },
                                font: {
                                    family: 'Times New Roman, Times, serif',
                                    weight: 'bold',
                                    size: 13
                                },
                                align: 'end',
                                anchor: 'end',
                                offset: -3, // sát chart luôn
                                display: function(context) {
                                    const label = context.chart.data.labels[context.dataIndex];
                                    const allowed = ['Dormant', 'Active', 'Non-CASA', 'Non-casa'];
                                    return allowed.includes(label);
                                },
                                formatter: function(value, context) {
                                    const label = context.chart.data.labels[context.dataIndex];
                                    const data = context.chart.data.datasets[0].data || [];
                                    const total = data.reduce((a, b) => a + b, 0);
                                    const pct = total ? (value / total) * 100 : 0;
                                    return `${label}\n${Math.round(value).toLocaleString()}\n${pct.toFixed(1)}%`;
                                }
                            }
                        },
                        clamp: false
                    }
                }
            },
            plugins: [ChartDataLabels]
        });
        console.log('✅ CASA Customer Donut chart created successfully');
    } catch (error) {
        console.error('❌ Error drawing CASA donut chart:', error);
        const chartContainer = document.getElementById('casa-customer-chart');
        if (chartContainer) {
            chartContainer.innerHTML = '<div style="display: flex; align-items: center; justify-content: center; height: 100%; color: #ef4444; font-size: 16px;">Error loading data</div>';
        }
    }
}

// Draw K-Study new open chart (Updated)
async function drawKStudyNewOpenChart() {
    try {
        const date = getCurrentDate();
        const region = document.getElementById('region-select')?.value || '';
        const branch = document.getElementById('branch-input')?.value || '';
        
        const ctx = document.getElementById('kstudy-newopen-chart');
        if (!ctx) return;
        
        if (window.kstudyNewOpenChart) {
            window.kstudyNewOpenChart.destroy();
        }

        const url = `${API_BASE_URL}/kstudy-newopen?date=${date}&region=${region}&branch=${branch}`;
        console.log('🔍 Fetching K-Study data from:', url);
        
        const res = await fetch(url);
        const data = await res.json();

        if (data.error || !data || !data.labels || data.labels.length === 0) {
            console.error('K-Study Chart: No data available');
            const context = ctx.getContext('2d');
            context.clearRect(0, 0, ctx.width, ctx.height);
            context.font = '16px Arial';
            context.fillStyle = '#666';
            context.textAlign = 'center';
            context.fillText('No data available', ctx.width / 2, ctx.height / 2);
            return;
        }

        console.log('📊 K-Study data:', data);

        // Filter out Y2022 if it exists and is the first element
        let filteredLabels = [...data.labels];
        let filteredData = [...data.data];

        if (filteredLabels[0] === 'Y2022') {
            filteredLabels.shift();
            filteredData.shift();
        }

        // Format x-axis labels: handle both year labels and month labels
        const formattedLabels = filteredLabels.map((label, idx) => {
            const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            
            // Check if it's already a formatted month label (e.g., "Jan 2024", "Feb 2025")
            if (label.includes(' ')) {
                return label; // Already formatted
            }
            
            // Check if it's a year label (e.g., "Y2023", "Y2024")
            if (label.startsWith('Y')) {
                return label; // Keep year labels as-is
            }
            
            // Check if it's a month name without year
            if (monthNames.includes(label)) {
                // It's a month - add current year from API response
                let year = data.years ? String(data.years.current_year) : '2025';
                if (data.dates_used && data.years) {
                    const currentYearKey = String(data.years.current_year);
                    if (data.dates_used[currentYearKey] && data.dates_used[currentYearKey][idx - 3]) {
                        // Adjust index for months (idx-3 because first 3 might be years)
                        const dateStr = data.dates_used[currentYearKey][idx - 3];
                        if (dateStr && dateStr.length >= 4) {
                            year = dateStr.slice(0, 4);
                        }
                    }
                }
                return `${label} ${year}`;
            }
            
            return label; // Keep other labels as-is
        });

        // Color coding: prev years = gray, current year months = dark blue
        const prevYear = data.years ? data.years.prev_year : 2024;
        const currentYear = data.years ? data.years.current_year : 2025;
        const colors = filteredLabels.map(label => {
            // Check if it's a year label (Y2023, Y2024, etc.) - make gray
            if (label.startsWith('Y') && parseInt(label.slice(1)) < currentYear) return '#b0b0b0';
            // Current year months = dark blue
            return '#223A5E';
        });

        window.kstudyNewOpenChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: formattedLabels,
                datasets: [{
                    label: 'New Open Accounts (Cumulative)',
                    data: filteredData,
                    backgroundColor: colors,
                    borderWidth: 1,
                    borderColor: '#fff',
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: { top: 40, bottom: 30 }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.raw;
                                return `New Accounts: ${value.toLocaleString()}`;
                            }
                        }
                    },
                    datalabels: {
                        color: '#000',
                        font: { 
                            family: 'Times New Roman, Times, serif', 
                            weight: 'bold', 
                            size: 18 
                        },
                        formatter: function(value) {
                            if (value === 0) return '';
                            return Math.round(value).toLocaleString();
                        },
                        align: 'end',
                        anchor: 'end',
                        offset: 8
                    }
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: {
                            font: { 
                                family: 'Times New Roman, Times, serif', 
                                weight: 'bold', 
                                size: 20 
                            },
                            color: '#111',
                            maxRotation: 45,
                            callback: function(value, index) {
                                // Show all labels, but rotate if too many
                                return this.getLabelForValue(value);
                            }
                        },
                        // XÓA title: { display: true, text: 'Period', ... }
                    },
                    y: { 
                        display: false,
                        beginAtZero: true,
                        max: function(context) {
                            // Calculate max value from data and add margin for data labels
                            const data = context.chart.data.datasets[0].data;
                            if (data && data.length > 0) {
                                const maxValue = Math.max(...data);
                                // Add larger margin (8%) to ensure data labels are fully visible
                                return maxValue + (maxValue * 0.08); // Add 8%
                            }
                            return undefined;
                        }
                    }
                }
            },
            plugins: [ChartDataLabels]
        });

        console.log('✅ K-Study new open chart created successfully');

    } catch (error) {
        console.error('❌ Error drawing K-study new open chart:', error);
        
        // Show error state
        const ctx = document.getElementById('kstudy-newopen-chart');
        if (ctx) {
            const context = ctx.getContext('2d');
            context.clearRect(0, 0, ctx.width, ctx.height);
            context.font = '16px Arial';
            context.fillStyle = '#ef4444';
            context.textAlign = 'center';
            context.fillText('Error loading data', ctx.width / 2, ctx.height / 2);
        }
    }
}


// Draw daily balance chart
async function drawDailyBalanceChart() {
    try {
        const date = getCurrentDate();
        const region = document.getElementById('region-select')?.value || '';
        const branch = document.getElementById('branch-input')?.value || '';
        const product = document.getElementById('product-filter')?.value || 'ALL';
        
        // Calculate start date (30 days before the input date)
        const inputDate = new Date(date);
        const startDate = new Date(inputDate);
        startDate.setDate(inputDate.getDate() - 30);
        const startDateStr = startDate.toISOString().slice(0, 10);
        
        const ctx = document.getElementById('daily-balance-chart');
        if (!ctx) return;
        
        if (window.dailyBalanceChart) {
            window.dailyBalanceChart.destroy();
        }

        const url = `${API_BASE_URL}/daily-balance-chart?date=${date}&start_date=${startDateStr}&region=${region}&branch=${branch}&product=${product}`;
        const res = await fetch(url);
        const data = await res.json();

        if (data.error || !data || !data.labels || data.labels.length === 0) {
            const context = ctx.getContext('2d');
            context.clearRect(0, 0, ctx.width, ctx.height);
            return;
        }

        // UPDATE: Backend đã format labels sẵn, không cần format lại
        const formattedLabels = data.labels; // Already formatted as "5 May", "6 May", etc.

        // UPDATE: Update chart title with actual period
        // Chỉ update tiêu đề đúng phần chứa daily-balance-chart
        const chartSection = ctx.closest('.chart-section');
        if (chartSection) {
            const h3 = chartSection.querySelector('h3');
            if (h3 && h3.textContent.includes('Ending Balance')) {
                h3.innerHTML = `<strong>Ending Balance - Last 30 Days</strong>`;
            }
            // XÓA legend phụ period
            let periodLegend = chartSection.querySelector('.period-legend');
            if (periodLegend) periodLegend.remove();
        }

        const maxVal = Math.max(...data.data);
        const minVal = Math.min(...data.data);
        const barColors = data.data.map(v =>
            v === maxVal ? '#009688' : v === minVal ? '#b0b0b0' : '#223A5E'
        );

        const isCasa = product === '1-CASA';
        window.dailyBalanceChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: formattedLabels,
                datasets: [{
                    label: `Daily Balance - ${product}`,
                    data: data.data,
                    backgroundColor: barColors,
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: { top: 80, bottom: 30 }
                },
                plugins: {
                    legend: { display: false },
                    datalabels: {
                        color: '#000',
                        font: { weight: 'bold', size: 12 },
                        formatter: function(value) {
                            return Math.round(value).toLocaleString();
                        },
                        align: 'end',
                        anchor: 'end',
                        offset: 8,
                        clamp: false
                    }
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: {
                            font: { weight: 'bold', size: 14 },
                            color: '#111',
                            maxRotation: 45, // Allow rotation for longer labels
                            callback: function(value, index) {
                                // Luôn hiển thị tất cả các ngày
                                return this.getLabelForValue(value);
                            }
                        },
                    },
                    y: { display: false }
                }
            },
            plugins: [ChartDataLabels]
        });
    } catch (error) {
        console.error('Error drawing daily balance chart:', error);
    }
}

console.log('Canvas elements:', {
  all: document.getElementById('gauge-all'),
  casa: document.getElementById('gauge-casa'),
  tda: document.getElementById('gauge-tda')
});
console.log('Render function exists:', typeof renderSimpleYoYGauge);

const testAPIs = async () => {
  const date = new Date().toISOString().slice(0,10);
  console.log('Testing with date:', date);
  try {
    const [allRes, casaRes, tdaRes] = await Promise.all([
      fetch(`${API_BASE_URL}/gauge-all?date=${date}`),
      fetch(`${API_BASE_URL}/gauge-data?date=${date}`),
      fetch(`${API_BASE_URL}/gauge-metrics?date=${date}`)
    ]);
    const allData = await allRes.json();
    const casaData = await casaRes.json();
    const tdaData = await tdaRes.json();
    console.log('🔍 API Responses:', { all: allData, casa: casaData, tda: tdaData });
    return { allData, casaData, tdaData };
  } catch (error) {
    console.error('❌ API Test Failed:', error);
  }
};
testAPIs();

if (typeof renderSimpleYoYGauge === 'function') {
  renderSimpleYoYGauge('gauge-all', 150000000, 300000000, '#22AFFF');
  renderSimpleYoYGauge('gauge-casa', 40000000, 100000000, '#10B981');
  renderSimpleYoYGauge('gauge-tda', 110000000, 100000000, '#F47B20');
}

const testGaugeRender = () => {
  const canvas1 = document.getElementById('gauge-all');
  const canvas2 = document.getElementById('gauge-casa');
  const canvas3 = document.getElementById('gauge-tda');
  if (canvas1 && canvas2 && canvas3) {
    const drawTest = (canvas, color, text) => {
      const ctx = canvas.getContext('2d');
      canvas.width = 280;
      canvas.height = 200;
      ctx.clearRect(0, 0, 280, 200);
      ctx.beginPath();
      ctx.arc(140, 120, 80, Math.PI, 0, false);
      ctx.lineWidth = 15;
      ctx.strokeStyle = '#e5e7eb';
      ctx.stroke();
      ctx.beginPath();
      ctx.arc(140, 120, 80, Math.PI, Math.PI * 0.6, false);
      ctx.lineWidth = 15;
      ctx.strokeStyle = color;
      ctx.stroke();
      ctx.font = 'bold 24px Arial';
      ctx.fillStyle = color;
      ctx.textAlign = 'center';
      ctx.fillText(text, 140, 120);
    };
    drawTest(canvas1, '#22AFFF', '150M');
    drawTest(canvas2, '#10B981', '40M');
    drawTest(canvas3, '#F47B20', '110M');
    console.log('🎨 Test render complete!');
  } else {
    console.error('❌ Canvas elements not found');
  }
};
testGaugeRender();

const date = new Date().toISOString().slice(0,10);
Promise.all([
  fetch(`${API_BASE_URL}/gauge-all?date=${date}`),
  fetch(`${API_BASE_URL}/gauge-data?date=${date}`),
  fetch(`${API_BASE_URL}/gauge-metrics?date=${date}`)
]).then(async ([allRes, casaRes, tdaRes]) => {
  const allData = await allRes.json();
  const casaData = await casaRes.json();
  const tdaData = await tdaRes.json();
  console.log('🔍 API Responses:', { all: allData, casa: casaData, tda: tdaData });
});

// Test 1: Check canvas elements
console.log('Canvas check:', {
  all: document.getElementById('gauge-all'),
  casa: document.getElementById('gauge-casa'),
  tda: document.getElementById('gauge-tda')
});

// Test 2: Manual gauge rendering
testGaugeRendering();

// Test 3: API test
testGaugeAPIs();

// Test 4: Force gauge update
updateAllGauges();
updateAllGauges();

function renderStackedBarCustomLegend(chart) {
    const legendContainer = document.getElementById('stacked-bar-custom-legend');
    if (!legendContainer || !chart) return;

    // Lấy thông tin datasets
    const datasets = chart.data.datasets;
    // Extract years from dataset labels dynamically
    const years = new Set();
    datasets.forEach(ds => {
        const match = ds.label.match(/(\d{4})$/);
        if (match) years.add(parseInt(match[1]));
    });
    const yearArray = Array.from(years).sort();
    const prevYear = yearArray[0] || 2024;
    const currentYear = yearArray[1] || 2025;
    
    // Đúng thứ tự: CASA prevYear, CASA currentYear, TDA prevYear, TDA currentYear, ISA prevYear, ISA currentYear
    const getDs = (label) => datasets.find(ds => ds.label === label);
    const items = [
        getDs(`1-CASA ${prevYear}`), getDs(`1-CASA ${currentYear}`),
        getDs(`2-TIME ${prevYear}`), getDs(`2-TIME ${currentYear}`),
        getDs(`3-ISA ${prevYear}`),  getDs(`3-ISA ${currentYear}`)
    ];
    // Đổi nhãn TIME thành TDA
    const labelMap = {
        '1-CASA': 'CASA',
        '2-TIME': 'TDA',
        '3-ISA': 'ISA'
    };
    function getLabel(label) {
        if (!label) return '';
        const [prefix, year] = label.split(' ');
        return `${labelMap[prefix] || prefix} ${year}`;
    }
    // Chia thành 2 cột, 3 dòng
    const col1 = [items[0], items[2], items[4]];
    const col2 = [items[1], items[3], items[5]];

    legendContainer.innerHTML = `
      <div class="custom-legend-2col" style="display: flex; flex-direction: row; justify-content: center; gap: 32px; margin-top: 4px !important; font-family: 'Times New Roman', Times, serif;">
        <div style="display: flex; flex-direction: column; gap: 4px;">
          ${col1.map(item => item ? `
            <div class="custom-legend-item" style="display: flex; align-items: center; gap: 10px; background: #f8fafc; border-radius: 8px; padding: 8px 20px; font-weight: 500; font-size: 15px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border: none; min-width: 120px; font-family: 'Times New Roman', Times, serif;">
              <span class="custom-legend-color" style="width: 18px; height: 18px; border-radius: 4px; background: ${item.backgroundColor}; border: 2px solid #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.10);"></span>
              <span>${getLabel(item.label)}</span>
            </div>
          ` : '').join('')}
        </div>
        <div style="display: flex; flex-direction: column; gap: 4px;">
          ${col2.map(item => item ? `
            <div class="custom-legend-item" style="display: flex; align-items: center; gap: 10px; background: #f8fafc; border-radius: 8px; padding: 8px 20px; font-weight: 500; font-size: 15px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border: none; min-width: 120px; font-family: 'Times New Roman', Times, serif;">
              <span class="custom-legend-color" style="width: 18px; height: 18px; border-radius: 4px; background: ${item.backgroundColor}; border: 2px solid #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.10);"></span>
              <span>${getLabel(item.label)}</span>
            </div>
          ` : '').join('')}
        </div>
      </div>
    `;
}

const canvas = document.getElementById('casa-customer-chart');
if (canvas) {
    canvas.width = 660;
    canvas.height = 660;
}

// Update the global export
window.drawCasaCustomerChart = drawCasaCustomerChart;
window.drawCasaSunburstChart = drawCasaCustomerChart;