/**
 * Card Business Dashboard - Data from API (api_server.py port 8700)
 * Auto-detect server IP for cross-machine access (works with file:// and offline)
 */
const API_BASE_URL = (() => {
    const isFile = window.location.protocol === 'file:';
    const hostname = window.location.hostname || 'localhost';
    const protocol = isFile ? 'http:' : window.location.protocol;
    return `${protocol}//${hostname}:8700`;
})();

let charts = {};
let currentData = null;
let currentCustomerType = 'all';
let flatpickrInstance = null;
/** In-memory cache: key = date|customerType, value = { data, ts }. Skip refetch when same params. */
let dashboardCache = null;
const CACHE_MAX_AGE_MS = 2 * 60 * 1000; // 2 minutes

// Chart.js global defaults - rõ nét, dễ đọc
Chart.defaults.font.family = "'Segoe UI', 'Inter', system-ui, sans-serif";
Chart.defaults.font.size = 13;
Chart.defaults.font.weight = '500';
Chart.defaults.color = '#1f2937';

const datalabelsPlugin = typeof ChartDataLabels !== 'undefined' ? ChartDataLabels : null;
if (datalabelsPlugin) Chart.register(datalabelsPlugin);

// Exact colors per design spec
const colors = {
    navy: '#2F5597',
    blue: '#4472C4',
    lightBlue: '#B4C7E7',
    veryLightBlue: '#DAE3F3',
    gold: '#BF9000',
    lightGold: '#F4B084',
    grey: '#7F7F7F',
    lightGrey: '#D0CECE',
    periwinkle: '#7B8AD7',
    orchid: '#D586BD',
    balanceBlue: '#20558A',
    revolvingGold: '#E2B13C',
    ticketGreen: '#5B9D4A',
    green: '#70AD47',
    orange: '#FF8C00',
    red: '#DC2626',
    // New Issuance vs. Termination palette
    primaryBlue: '#20538D',
    secondaryBlue: '#8DB4E2',
    neutralGrey: '#A5A5A5',
    accentGold: '#E2C08D'
};

function formatNumber(num) {
    if (num == null || num === undefined || num === '') return '-';
    const n = typeof num === 'number' ? num : parseFloat(num);
    if (isNaN(n)) return '-';
    return n.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

function formatPct(p) {
    if (p == null || p === undefined) return '-';
    const n = typeof p === 'number' ? p : parseFloat(p);
    if (isNaN(n)) return '-';
    return n.toFixed(2) + '%';
}

async function fetchWithTimeout(url, options = {}, timeout = 10000) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    try {
        const response = await fetch(url, {
            ...options,
            signal: controller.signal
        });
        clearTimeout(id);
        return response;
    } catch (error) {
        clearTimeout(id);
        if (error.name === 'AbortError') {
            throw new Error('Request timeout - server may not be running');
        }
        throw error;
    }
}

async function fetchDashboardData(date, customerType) {
    const dt = date || document.getElementById('date-picker')?.value;
    const ct = customerType != null ? customerType : currentCustomerType;
    const cacheKey = (dt || '') + '|' + (ct || 'all');
    if (dashboardCache && dashboardCache.key === cacheKey && (Date.now() - dashboardCache.ts) < CACHE_MAX_AGE_MS) {
        return dashboardCache.data;
    }
    try {
        const params = new URLSearchParams();
        if (dt) params.set('date', dt);
        if (ct) params.set('customer_type', ct);
        const url = `${API_BASE_URL}/api/dashboard/full-stats?${params.toString()}`;
        console.log('Fetching data from:', url);
        const response = await fetchWithTimeout(url, {}, 15000);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        dashboardCache = { key: cacheKey, data, ts: Date.now() };
        currentData = data;
        return data;
    } catch (error) {
        console.error('Error fetching dashboard data:', error);
        const errorMsg = error.message || 'Unknown error';
        if (errorMsg.includes('Failed to fetch') ||
            errorMsg.includes('NetworkError') ||
            errorMsg.includes('Connection refused') ||
            errorMsg.includes('timeout')) {
            console.warn('⚠️ API server may not be running. Please start it with: python api_server.py');
            console.warn('Server should be running on: http://localhost:8700');
            return null;
        }
        alert(`Error loading dashboard data.\n\nError: ${errorMsg}\n\nPlease check:\n1. API server is running on port 8700\n2. Database is accessible\n3. Check browser console (F12) for details.`);
        return null;
    }
}

async function fetchAvailableDates() {
    try {
        const response = await fetchWithTimeout(`${API_BASE_URL}/api/dashboard/available-dates`, {}, 5000);
        if (!response.ok) return [];
        const data = await response.json();
        return data.dates || [];
    } catch (error) {
        if (error.message && (error.message.includes('Failed to fetch') || error.message.includes('timeout'))) {
            console.warn('⚠️ API server may not be running. Available dates unavailable.');
        } else {
            console.warn('Available dates failed:', error);
        }
        return [];
    }
}

function updateTickers(data) {
    if (!data || !data.tickers) return;
    for (let i = 1; i <= 5; i++) {
        const t = data.tickers['ticker_' + i];
        if (!t) continue;
        const valEl = document.getElementById('kpi-' + i + '-value');
        const chgEl = document.getElementById('kpi-' + i + '-change');
        const tgtEl = document.getElementById('kpi-' + i + '-target');
        if (valEl) {
            const v = t.value;
            valEl.textContent = (i === 4 || i === 5) ? formatNumber(Math.round(v)) : formatNumber(v);
        }
        if (chgEl) {
            const m = t.vs_last_month != null ? t.vs_last_month : 0;
            const y = t.vs_last_year != null ? t.vs_last_year : 0;
            const mSym = m >= 0 ? '▲' : '▼';
            const ySym = y >= 0 ? '▲' : '▼';
            const mColor = m >= 0 ? '#27AE60' : '#C0392B';
            const yColor = y >= 0 ? '#27AE60' : '#C0392B';
            // Một dòng: vs Last Month + vs Last Year, font 11px
            chgEl.innerHTML =
                `<span style="font-size:11px;color:${mColor}">${mSym} ${Math.abs(m).toFixed(2)}% vs Last Month</span>` +
                ` &nbsp;|&nbsp; ` +
                `<span style="font-size:11px;color:${yColor}">${ySym} ${Math.abs(y).toFixed(2)}% vs Last Year</span>`;
        }
        if (tgtEl) {
            const mt = t.month_target_pct != null ? t.month_target_pct + '%' : '-';
            const yt = t.year_target_pct != null ? t.year_target_pct + '%' : '-';
            tgtEl.innerHTML = `${mt} of Month Target<br>${yt} of Y2026 Target`;
        }
    }
}

function updateDelinquency(data) {
    if (!data || !data.risk_analysis) return;
    const cards = data.risk_analysis.cards || {};
    const comp = data.risk_analysis.comparisons || {};
    const ids = [
        { key: 'dpd_1', el: 'risk-dpd-1', cmp: 'risk-dpd-1-cmp' },
        { key: 'dpd_10', el: 'risk-dpd-10', cmp: 'risk-dpd-10-cmp' },
        { key: 'dpd_30', el: 'risk-dpd-30', cmp: 'risk-dpd-30-cmp' },
        { key: 'npl', el: 'risk-npl', cmp: 'risk-npl-cmp' }
    ];
    ids.forEach(({ key, el, cmp }) => {
        const v = cards[key];
        const c = comp[key];
        const elEl = document.getElementById(el);
        const cmpEl = document.getElementById(cmp);
        if (elEl) elEl.textContent = v != null ? formatPct(v) : '-';
        if (cmpEl && c) {
            const d = c.pct_change_vs_last_date != null ? c.pct_change_vs_last_date : 0;
            const m = c.pct_change_vs_last_month != null ? c.pct_change_vs_last_month : 0;
            const dSym = d >= 0 ? '▲' : '▼';
            const mSym = m >= 0 ? '▲' : '▼';
            // Quy ước: tăng = xanh lá, giảm = đỏ
            const dColor = d >= 0 ? '#27AE60' : '#C0392B';
            const mColor = m >= 0 ? '#27AE60' : '#C0392B';
            cmpEl.innerHTML = `<span style="font-size:18px;color:${dColor}">${dSym} ${Math.abs(d).toFixed(2)}% vs Last Day</span><br><span style="font-size:18px;color:${mColor}">${mSym} ${Math.abs(m).toFixed(2)}% vs Last Month</span>`;
        }
    });
}

function destroyCharts() {
    Object.values(charts).forEach(c => { if (c) c.destroy(); });
    charts = {};
}

function getMonths(data) {
    return data?.meta?.months_label || [];
}

function buildCharts(data) {
    if (!data) return;
    destroyCharts();
    
    // ========================================================================
    // 6-MONTH HISTORICAL CHARTS
    // ========================================================================
    // All charts below display 6 months of data (5 previous months + current).
    // Data source: backend returns arrays aligned to `data.meta.months_label`.
    // Frontend logic:
    // - Keep only last 6 points via `startIndex` + `slice(startIndex)`
    // - X-axis labels are formatted as:
    //   - Previous months: "Mon YYYY" (e.g., "Jan 2026")
    //   - Selected date: "D Mon YYYY" (e.g., "14 Jan 2026")
    // Compatibility:
    // - If DB has < 6 months, charts will render with available points.
    // ========================================================================
    const months = getMonths(data);
    const ch = data.charts || {};
    const nc = ch.new_closed || {};
    const mg = ch.member_growth || {};
    const ft = ch.financial_trends || {};
    const rev = ch.revolving || {};
    const pie = ch.pie_snapshot || {};

    const newConsumerFull = nc.new_consumer || [];
    const newCorporateFull = nc.new_corporate || [];
    const newConsumerSmartCreditFull = nc.new_consumer_smartcredit || [];
    const newConsumerBranchesFull = nc.new_consumer_branches || [];
    const newCorporateSmartCreditFull = nc.new_corporate_smartcredit || [];
    const newCorporateBranchesFull = nc.new_corporate_branches || [];
    const consumerClosedFull = nc.consumer_closed || [];
    const corporateClosedFull = nc.corporate_closed || [];

    // Always show last 6 points: last date of 5 previous months + selected date
    const maxPoints = 6;
    const totalPoints = months.length;
    const startIndex = totalPoints > maxPoints ? totalPoints - maxPoints : 0;
    const labels = months.slice(startIndex);
    const sliceSeries = (arr) => Array.isArray(arr) ? arr.slice(startIndex) : [];

    const isAll = currentCustomerType === 'all';
    const isConsumer = currentCustomerType === 'consumer';
    const isCorporate = currentCustomerType === 'corporate';

    const newTotalFull = months.map((_, i) => (newConsumerFull[i] || 0) + (newCorporateFull[i] || 0));
    const closedTotalFull = months.map((_, i) => (consumerClosedFull[i] || 0) + (corporateClosedFull[i] || 0));
    const newConsumer = sliceSeries(newConsumerFull);
    const newCorporate = sliceSeries(newCorporateFull);
    const newTotal = sliceSeries(newTotalFull);
    const closedTotal = sliceSeries(closedTotalFull);

    // In & Out: theo customer_type
    // - all: New Issuance = new_consumer + new_corporate, Termination = consumer_closed + corporate_closed
    // - consumer: New Issuance = new_consumer, Termination = consumer_closed
    // - corporate: New Issuance = new_corporate, Termination = corporate_closed
    let inOutNewSeries;
    let inOutClosedSeries;
    if (isAll) {
        inOutNewSeries = newTotal;
        inOutClosedSeries = closedTotal;
    } else if (isConsumer) {
        inOutNewSeries = newConsumer;
        inOutClosedSeries = sliceSeries(consumerClosedFull);
    } else {
        inOutNewSeries = newCorporate;
        inOutClosedSeries = sliceSeries(corporateClosedFull);
    }

    // New Acquisition: luôn chia Smart Credit vs Branches (theo Metric_Code)
    // - all: Smart Credit = New_Consumer_SmartCredit + New_Corporate_SmartCredit, Branches = New_Consumer_Branches + New_Corporate_Branches, Total = New_Member
    // - consumer: Smart Credit = New_Consumer_SmartCredit, Branches = New_Consumer_Branches, Total = New_Consumer
    // - corporate: Smart Credit = New_Corporate_SmartCredit, Branches = New_Corporate_Branches, Total = New_Corporate
    let newAcqSeries1;
    let newAcqSeries2;
    let newAcqTotalSeries;
    if (isAll) {
        newAcqSeries1 = sliceSeries(months.map((_, i) => (newConsumerSmartCreditFull[i] || 0) + (newCorporateSmartCreditFull[i] || 0)));
        newAcqSeries2 = sliceSeries(months.map((_, i) => (newConsumerBranchesFull[i] || 0) + (newCorporateBranchesFull[i] || 0)));
        newAcqTotalSeries = newTotal;
    } else if (isConsumer) {
        newAcqSeries1 = sliceSeries(newConsumerSmartCreditFull);
        newAcqSeries2 = sliceSeries(newConsumerBranchesFull);
        newAcqTotalSeries = newConsumer;
    } else {
        newAcqSeries1 = sliceSeries(newCorporateSmartCreditFull);
        newAcqSeries2 = sliceSeries(newCorporateBranchesFull);
        newAcqTotalSeries = newCorporate;
    }
    const eopConsumerFull = mg.eop_consumer || [];
    const eopCorporateFull = data.table_data?.eop_corporate || [];
    const usingConsumerFull = mg.using_consumer || [];
    const usingCorporateFull = data.table_data?.using_corporate || [];

    const eopFull = isAll
        ? months.map((_, i) => (eopConsumerFull[i] || 0) + (eopCorporateFull[i] || 0))
        : (isConsumer ? eopConsumerFull : eopCorporateFull);
    const usingFull = isAll
        ? months.map((_, i) => (usingConsumerFull[i] || 0) + (usingCorporateFull[i] || 0))
        : (isConsumer ? usingConsumerFull : usingCorporateFull);

    const eop = sliceSeries(eopFull);
    const using = sliceSeries(usingFull);

    const outstandingFull = ft.outstanding || [];
    const spendingFull = ft.spending || [];
    const revBalanceFull = rev.balance || [];
    const outstandingKFull = outstandingFull.map(v => (v || 0) / 1000); // Balance luôn quy về K USD
    const revBalanceKFull = revBalanceFull; // Revolving trong DB đã /1000 sẵn → giữ nguyên
    const outstanding = sliceSeries(outstandingKFull);
    const spending = sliceSeries(spendingFull);
    const revBalance = sliceSeries(revBalanceKFull);
    const balanceNonRev = labels.map((_, idx) => {
        const i = startIndex + idx;
        return Math.max(0, (outstandingKFull[i] || 0) - (revBalanceKFull[i] || 0));
    });

    const avgSpendFull = (data.table_data && data.table_data.avg_spending) ? data.table_data.avg_spending : [];
    const avgSpend = sliceSeries(avgSpendFull);

    // Build axis dates: last 5 months (relative) + selected date
    let axisDates = [];
    const queryDateStr = data?.meta?.query_date;
    if (queryDateStr) {
        const [qy, qm, qd] = queryDateStr.split('-').map(Number);
        const base = new Date(qy, (qm || 1) - 1, qd || 1);
        axisDates = labels.map((_, idx) => {
            const d = new Date(base);
            const offset = idx - (labels.length - 1); // last index = selected date
            d.setMonth(d.getMonth() + offset);
            return d;
        });
    }

    const formatAxisLabel = (index) => {
        const d = axisDates[index];
        const fallback = labels[index] ?? '';
        if (!d || Number.isNaN(d.getTime())) return fallback;
        const year = d.getFullYear();
        const monthShort = d.toLocaleString('en-US', { month: 'short' });
        if (index === labels.length - 1) {
            const day = d.getDate(); // không cần 0 ở trước
            // ví dụ: 14 Jan 2026
            return `${day} ${monthShort} ${year}`;
        }
        // ví dụ: Jan 2025
        return `${monthShort} ${year}`;
    };
    // Data label: chia hàng ngàn bằng dấu phẩy (không thập phân)
    const formatLabelComma = (d) => { if (d == null || isNaN(Number(d))) return ''; return Math.round(Number(d)).toLocaleString('en-US'); };
    const formatLabelInt = (d) => { if (d == null || isNaN(Number(d))) return ''; return Math.round(Number(d)).toLocaleString('en-US'); };

    const sector = pie.sector || {};
    const loc = pie.location || {};
    const chan = pie.channel || {};
    const sum = (a, b) => (a || 0) + (b || 0);
    const totSector = sum(sector.purchasing, sector.installment) + sum(sector.cash, sector.flexi) || 1;
    const totLoc = sum(loc.domestic, loc.x_border) || 1;
    const totChan = sum(chan.pos, chan.online) || 1;

    const sectorColors = ['#5B8FC4', '#EDD07A', '#FFB366', '#E5CA6A'];
    const pieOptionsBase = {
        responsive: true, maintainAspectRatio: false,
        layout: { padding: { bottom: 40 } },
        cutout: '58%',
        spacing: 2,
        plugins: {
            legend: { display: false },
            tooltip: { callbacks: { label: ctx => ctx.label + ': ' + Math.round(ctx.parsed) + '%' } }
        }
    };

    // 1. New Acquisition (stacked Smart Credit + Branches, line Total) - bar 40-50% width, square corners
    const newAcqCtx = document.getElementById('newAcqChart');
    if (newAcqCtx) {
        charts.newAcq = new Chart(newAcqCtx.getContext('2d'), {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    { label: 'Smart Credit', data: newAcqSeries1, backgroundColor: colors.primaryBlue, stack: 'stack1', borderRadius: 0,
                        datalabels: { color: 'rgba(255,255,255,0.9)', font: { size: 16, weight: 'normal' }, anchor: 'center', align: 'center',
                            formatter: (v, ctx) => formatLabelInt(ctx.dataset.data[ctx.dataIndex]),
                            display: ctx => { const val = ctx.dataset.data[ctx.dataIndex]; return val != null && Number(val) !== 0; } } },
                    { label: 'Branches', data: newAcqSeries2, backgroundColor: colors.secondaryBlue, stack: 'stack1', borderRadius: 0,
                        datalabels: { color: '#1a1a1a', font: { size: 16, weight: 'normal' },
                            anchor: (ctx) => { const last = ctx.dataset.data.length - 1; return ctx.dataIndex === last ? 'end' : 'center'; },
                            align: (ctx) => { const last = ctx.dataset.data.length - 1; return ctx.dataIndex === last ? 'top' : 'center'; },
                            offset: (ctx) => { const last = ctx.dataset.data.length - 1; return ctx.dataIndex === last ? [0, 6] : 0; },
                            backgroundColor: '#FFFFFF', padding: 4, borderRadius: 2,
                            formatter: (v, ctx) => formatLabelInt(ctx.dataset.data[ctx.dataIndex]),
                            display: ctx => { const val = ctx.dataset.data[ctx.dataIndex]; return val != null && Number(val) !== 0; } } },
                    { label: 'Total', data: newAcqTotalSeries, type: 'line', borderColor: colors.grey, borderWidth: 2, borderDash: [6, 4], fill: false, pointRadius: 0, yAxisID: 'y1',
                        datalabels: {
                            color: 'rgba(0,0,0,0.85)',
                            font: { size: 16, weight: 'normal' },
                            anchor: 'end',
                            align: 'top',
                            offset: (ctx) => {
                                const last = ctx.dataset.data.length - 1;
                                return ctx.dataIndex === last ? 28 : 12;
                            },
                            formatter: (v, ctx) => formatLabelInt(ctx.dataset.data[ctx.dataIndex]),
                            display: ctx => { const val = ctx.dataset.data[ctx.dataIndex]; return val != null && Number(val) !== 0; }
                        } }
                ]
            },
            ...(datalabelsPlugin && { plugins: [datalabelsPlugin] }),
            options: {
                responsive: true, maintainAspectRatio: false,
                layout: { padding: { top: 40, bottom: 80 } },
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: true, position: 'bottom', padding: 32, labels: { boxWidth: 12, padding: 8, font: { size: 15, weight: 'normal' } } },
                    tooltip: { backgroundColor: 'rgba(0,0,0,0.8)', padding: 10, titleFont: { size: 11 }, bodyFont: { size: 10 } },
                    datalabels: { clamp: false }
                },
                scales: {
                    x: {
                        stacked: true,
                        display: true,
                        grid: { display: false },
                        barPercentage: 0.5,
                        categoryPercentage: 0.8,
                        ticks: {
                            font: { size: 14, weight: 'normal' },
                            callback: (_, idx) => formatAxisLabel(idx)
                        }
                    },
                    y: { stacked: true, beginAtZero: true, display: false, grid: { display: false } },
                    y1: { beginAtZero: true, display: false, grid: { display: false } }
                }
            }
        });
    }

    // 2. In & Out - Clustered bar (New Issuance #20538D, Termination #A5A5A5), gap 2-4px between pairs
    const inOutCtx = document.getElementById('inOutChart');
    if (inOutCtx) {
        charts.inOut = new Chart(inOutCtx.getContext('2d'), {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    { label: 'New Issuance', data: inOutNewSeries, backgroundColor: colors.primaryBlue, borderRadius: 0,
                        datalabels: { color: colors.primaryBlue, font: { size: 16 }, anchor: 'end', align: 'top', offset: 6,
                            formatter: (v, ctx) => formatLabelInt(ctx.dataset.data[ctx.dataIndex]), display: ctx => ctx.dataset.data[ctx.dataIndex] != null } },
                    { label: 'Termination', data: inOutClosedSeries, backgroundColor: colors.neutralGrey, borderRadius: 0,
                        datalabels: { color: colors.neutralGrey, font: { size: 16 }, anchor: 'end', align: 'top', offset: 6,
                            formatter: (v, ctx) => formatLabelInt(ctx.dataset.data[ctx.dataIndex]), display: ctx => ctx.dataset.data[ctx.dataIndex] != null } }
                ]
            },
            ...(datalabelsPlugin && { plugins: [datalabelsPlugin] }),
            options: {
                responsive: true, maintainAspectRatio: false,
                layout: { padding: { top: 40, bottom: 80 } },
                plugins: {
                    legend: { display: true, position: 'bottom', padding: 32, labels: { boxWidth: 12, padding: 8, font: { size: 15, weight: 'normal' } } },
                    datalabels: { clamp: false }
                },
                scales: {
                    x: {
                        display: true,
                        grid: { display: false },
                        barPercentage: 0.9,
                        categoryPercentage: 0.85,
                        ticks: {
                            font: { size: 14, weight: 'normal' },
                            callback: (_, idx) => formatAxisLabel(idx)
                        }
                    },
                    y: { beginAtZero: true, display: false, grid: { display: false } }
                }
            }
        });
    }

    // 3. Customer Base & Using Mem - #7B8AD7 (Periwinkle), #D586BD (Orchid)
    const custBaseCtx = document.getElementById('custBaseChart');
    if (custBaseCtx) {
        charts.custBase = new Chart(custBaseCtx.getContext('2d'), {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    { label: 'Customer Base', data: eop, backgroundColor: colors.periwinkle, borderRadius: { topLeft: 2, topRight: 2 },
                        datalabels: { color: '#444444', font: { size: 16 }, anchor: 'end', align: 'top', offset: 6,
                            formatter: (v, ctx) => formatLabelInt(ctx.dataset.data[ctx.dataIndex]), display: ctx => ctx.dataset.data[ctx.dataIndex] != null } },
                    { label: 'Using Mem', data: using, backgroundColor: colors.orchid, borderRadius: { topLeft: 2, topRight: 2 },
                        datalabels: { color: '#444444', font: { size: 16 }, anchor: 'end', align: 'top', offset: 6,
                            formatter: (v, ctx) => formatLabelInt(ctx.dataset.data[ctx.dataIndex]), display: ctx => ctx.dataset.data[ctx.dataIndex] != null } }
                ]
            },
            ...(datalabelsPlugin && { plugins: [datalabelsPlugin] }),
            options: {
                responsive: true, maintainAspectRatio: false,
                layout: { padding: { top: 40, bottom: 80 } },
                plugins: {
                    legend: { display: true, position: 'bottom', padding: 32, labels: { boxWidth: 12, padding: 10, font: { size: 16, weight: 'normal' } } },
                    tooltip: { callbacks: { label: c => c.dataset.label + ': ' + (c.raw != null ? formatLabelInt(c.raw) : '-') } },
                    datalabels: { clamp: false }
                },
                scales: {
                    x: {
                        display: true,
                        grid: { display: false },
                        barPercentage: 0.8,
                        categoryPercentage: 0.6,
                        ticks: {
                            font: { size: 14, weight: 'normal' },
                            callback: (_, idx) => formatAxisLabel(idx)
                        }
                    },
                    y: { beginAtZero: false, display: false, grid: { display: false } }
                }
            }
        });
    }

    // 4. Balance (K USD) - Clustered: Balance #20558A, Revolving #E2B13C — data label dạng K, không thập phân
    const balanceCtx = document.getElementById('balanceChart');
    if (balanceCtx) {
        // Dữ liệu đã ở đơn vị K USD (outstandingKFull, revBalanceKFull) nên không chia /1000 lần nữa
        // Balance hiển thị đúng tổng từ bảng 05 (Total_Outstanding_Balance / 1000), không trừ Revolving
        const balTotal = outstanding;
        const balRev = revBalance;
        charts.balance = new Chart(balanceCtx.getContext('2d'), {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    { label: 'Balance', data: balTotal, backgroundColor: colors.balanceBlue, borderRadius: { topLeft: 2, topRight: 2 },
                        datalabels: { color: '#444444', font: { size: 16 }, anchor: 'end', align: 'top', offset: 6,
                            formatter: (v, ctx) => formatLabelComma(ctx.dataset.data[ctx.dataIndex]), display: ctx => ctx.dataset.data[ctx.dataIndex] != null } },
                    { label: 'Revolving', data: balRev, backgroundColor: colors.revolvingGold, borderRadius: { topLeft: 2, topRight: 2 },
                        datalabels: { color: '#444444', font: { size: 16 }, anchor: 'end', align: 'top', offset: 6,
                            formatter: (v, ctx) => formatLabelComma(ctx.dataset.data[ctx.dataIndex]), display: ctx => ctx.dataset.data[ctx.dataIndex] != null } }
                ]
            },
            ...(datalabelsPlugin && { plugins: [datalabelsPlugin] }),
            options: {
                responsive: true, maintainAspectRatio: false,
                layout: { padding: { top: 40, bottom: 80 } },
                plugins: {
                    legend: { display: true, position: 'bottom', padding: 32, labels: { boxWidth: 12, padding: 10, font: { size: 16, weight: 'normal' } } },
                    tooltip: { callbacks: { label: c => c.dataset.label + ': ' + (c.raw != null ? formatLabelComma(c.raw) + ' K USD' : '-') } },
                    datalabels: { clamp: false }
                },
                scales: {
                    x: {
                        display: true,
                        grid: { display: false },
                        barPercentage: 0.8,
                        categoryPercentage: 0.6,
                        ticks: {
                            font: { size: 14, weight: 'normal' },
                            callback: (_, idx) => formatAxisLabel(idx)
                        }
                    },
                    y: { beginAtZero: true, display: false, grid: { display: false } }
                }
            }
        });
    }

    // 5. Sales Volume (K USD) - Gradient #3B82F6 (top) to #DBEAFE (bottom), no gridlines
    const volumeCtx = document.getElementById('volumeChart');
    if (volumeCtx) {
        const volCtx2d = volumeCtx.getContext('2d');
        const volGrad = volCtx2d.createLinearGradient(0, 0, 0, 300);
        volGrad.addColorStop(0, '#3B82F6');
        volGrad.addColorStop(1, '#DBEAFE');
        charts.volume = new Chart(volCtx2d, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: 'Total', data: spending.map(v => (v || 0) / 1000), backgroundColor: volGrad, borderRadius: { topLeft: 2, topRight: 2 },
                    datalabels: { color: '#444444', font: { size: 16 }, anchor: 'end', align: 'top', offset: 6,
                        formatter: (v, ctx) => formatLabelComma(ctx.dataset.data[ctx.dataIndex]), display: ctx => ctx.dataset.data[ctx.dataIndex] != null }
                }]
            },
            ...(datalabelsPlugin && { plugins: [datalabelsPlugin] }),
            options: {
                responsive: true, maintainAspectRatio: false,
                layout: { padding: { top: 40, bottom: 60 } },
                plugins: { legend: { display: false }, datalabels: { clamp: false } },
                scales: {
                    x: {
                        display: true,
                        grid: { display: false },
                        barPercentage: 0.7,
                        categoryPercentage: 0.75,
                        ticks: {
                            font: { size: 14, weight: 'normal' },
                            callback: (_, idx) => formatAxisLabel(idx)
                        }
                    },
                    y: { beginAtZero: true, display: false, grid: { display: false } }
                }
            }
        });
    }

    // 6. Spend per cus (USD) - Line #5B9D4A, square markers 6px
    const spendCtx = document.getElementById('spendChart');
    if (spendCtx) {
        charts.spend = new Chart(spendCtx.getContext('2d'), {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    label: 'Ticket size', data: avgSpend,
                    borderColor: colors.ticketGreen, backgroundColor: 'rgba(91, 157, 74, 0.08)', borderWidth: 2,
                    fill: true, tension: 0.3, pointStyle: 'rect', pointRadius: 6, pointBackgroundColor: colors.ticketGreen, pointBorderColor: colors.ticketGreen, pointBorderWidth: 1,
                    datalabels: { color: '#444444', font: { size: 16 }, anchor: 'end', align: 'top', offset: 8,
                        formatter: (v, ctx) => formatLabelInt(ctx.dataset.data[ctx.dataIndex]), display: ctx => ctx.dataset.data[ctx.dataIndex] != null }
                }]
            },
            ...(datalabelsPlugin && { plugins: [datalabelsPlugin] }),
            options: {
                responsive: true, maintainAspectRatio: false,
                layout: { padding: { top: 40, bottom: 60, left: 8, right: 8 } },
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: c => 'Ticket size: ' + (c.raw != null ? '$' + formatLabelInt(c.raw) : '-') } },
                    datalabels: { clamp: false }
                },
                scales: {
                    x: {
                        display: true,
                        grid: { display: false },
                        ticks: {
                            font: { size: 14, weight: 'normal' },
                            callback: (_, idx) => formatAxisLabel(idx)
                        }
                    },
                    y: { beginAtZero: false, display: false, grid: { display: false } }
                }
            }
        });
    }

    // Spending Distribution - Pie charts
    const sectorCtx = document.getElementById('sectorChart');
    if (sectorCtx) {
        const p = ((sector.purchasing || 0) / totSector) * 100;
        const i = ((sector.installment || 0) / totSector) * 100;
        const c = ((sector.cash || 0) / totSector) * 100;
        const f = ((sector.flexi || 0) / totSector) * 100;
        const sectorData = [p, i, c, f].map(x => x || 0.01);
        const sectorOpts = { ...pieOptionsBase, plugins: { ...pieOptionsBase.plugins,
            ...(datalabelsPlugin && { datalabels: { formatter: (v) => (v >= 5) ? Math.round(v) + '%' : '', color: '#333', font: { size: 14, weight: 'bold' }, display: (ctx) => ctx.dataset.data[ctx.dataIndex] >= 5 } })
        } };
        charts.sector = new Chart(sectorCtx.getContext('2d'), {
            type: 'doughnut',
            data: { labels: ['Purchasing', 'Installment', 'Cash Advance', 'Flexi'], datasets: [{ data: sectorData, backgroundColor: sectorColors, borderColor: '#fff', borderWidth: 2 }] },
            ...(datalabelsPlugin && { plugins: [datalabelsPlugin] }),
            options: sectorOpts
        });
    }

    const locationCtx = document.getElementById('locationChart');
    if (locationCtx) {
        const d = ((loc.domestic || 0) / totLoc) * 100;
        const x = ((loc.x_border || 0) / totLoc) * 100;
        const locOpts = { ...pieOptionsBase, plugins: { ...pieOptionsBase.plugins,
            ...(datalabelsPlugin && { datalabels: { formatter: (v) => (v != null && !isNaN(Number(v))) ? Math.round(Number(v)) + '%' : '', color: '#333', font: { size: 14, weight: 'bold' } } })
        } };
        charts.location = new Chart(locationCtx.getContext('2d'), {
            type: 'doughnut',
            data: { labels: ['Domestic', 'X-Border'], datasets: [{ data: [d, x].map(v => v || 0.01), backgroundColor: ['#9BA8E8', '#E4A8D0'], borderColor: '#fff', borderWidth: 2 }] },
            ...(datalabelsPlugin && { plugins: [datalabelsPlugin] }),
            options: locOpts
        });
    }

    const channelCtx = document.getElementById('channelChart');
    if (channelCtx) {
        const pos = ((chan.pos || 0) / totChan) * 100;
        const onl = ((chan.online || 0) / totChan) * 100;
        const chanOpts = { ...pieOptionsBase, plugins: { ...pieOptionsBase.plugins,
            ...(datalabelsPlugin && { datalabels: { formatter: (v) => (v != null && !isNaN(Number(v))) ? Math.round(Number(v)) + '%' : '', color: '#333', font: { size: 14, weight: 'bold' } } })
        } };
        charts.channel = new Chart(channelCtx.getContext('2d'), {
            type: 'doughnut',
            data: { labels: ['POS (Offline)', 'Online'], datasets: [{ data: [pos, onl].map(v => v || 0.01), backgroundColor: ['#5B8FC4', '#8BC47A'], borderColor: '#fff', borderWidth: 2 }] },
            ...(datalabelsPlugin && { plugins: [datalabelsPlugin] }),
            options: chanOpts
        });
    }
}

function setLoading(show) {
    const el = document.getElementById('dashboard-loading');
    if (el) el.classList.toggle('active', !!show);
}

async function loadDashboard() {
    const dateInput = document.getElementById('date-picker');
    const date = dateInput?.value || null;
    setLoading(true);
    try {
        const data = await fetchDashboardData(date);
        if (!data) {
            if (!date) {
                const dates = await fetchAvailableDates();
                if (dates.length && dateInput) dateInput.value = dates[0];
            }
            return;
        }
        currentData = data;
        updateTickers(data);
        updateDelinquency(data);
        buildCharts(data);
        if (dateInput && data.meta?.query_date) dateInput.value = data.meta.query_date;
    } catch (e) {
        console.error('Load dashboard error:', e);
        if (!date) {
            const dates = await fetchAvailableDates();
            if (dates.length && dateInput) dateInput.value = dates[0];
        }
    } finally {
        setLoading(false);
    }
}

async function setupDatePicker() {
    const input = document.getElementById('date-picker');
    if (!input) return;
    const dates = await fetchAvailableDates();
    if (dates.length) {
        const sorted = dates.sort();
        const latest = sorted[sorted.length - 1];
        flatpickrInstance = flatpickr(input, {
            dateFormat: 'Y-m-d',
            defaultDate: latest,
            enable: dates,
            onChange: function(selectedDates, dateStr) {
                if (dateStr) loadDashboard();
            }
        });
        input.value = latest;
    } else {
        flatpickrInstance = flatpickr(input, {
            dateFormat: 'Y-m-d',
            onChange: function(selectedDates, dateStr) {
                if (dateStr) loadDashboard();
            }
        });
    }
}

function setupCustomerTabs() {
    document.querySelectorAll('.filter-btn[data-type]').forEach(btn => {
        btn.addEventListener('click', function() {
            currentCustomerType = this.getAttribute('data-type');
            document.querySelectorAll('.filter-btn[data-type]').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            loadDashboard();
        });
    });
}

document.addEventListener('DOMContentLoaded', function() {
    setupDatePicker().then(() => {
        setupCustomerTabs();
        loadDashboard();
    });
});
