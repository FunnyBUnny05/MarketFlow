// Sector Z-Score Dashboard v4 - Simplified
// Single sector view with S&P 500 comparison

const SECTORS = [
    { ticker: 'XLB', name: 'Materials', color: '#f97316' },
    { ticker: 'XLE', name: 'Energy', color: '#3b82f6' },
    { ticker: 'XLF', name: 'Financials', color: '#a855f7' },
    { ticker: 'XLI', name: 'Industrials', color: '#06b6d4' },
    { ticker: 'XLK', name: 'Technology', color: '#10b981' },
    { ticker: 'XLP', name: 'Consumer Staples', color: '#f59e0b' },
    { ticker: 'XLU', name: 'Utilities', color: '#6366f1' },
    { ticker: 'XLV', name: 'Healthcare', color: '#ec4899' },
    { ticker: 'XLY', name: 'Consumer Disc', color: '#14b8a6' },
    { ticker: 'XLRE', name: 'Real Estate', color: '#8b5cf6' },
    { ticker: 'XLC', name: 'Communication', color: '#f43f5e' },
    { ticker: 'SMH', name: 'Semiconductors', color: '#22d3ee' },
    { ticker: 'XHB', name: 'Homebuilders', color: '#a3e635' },
    { ticker: 'XOP', name: 'Oil & Gas E&P', color: '#fbbf24' },
    { ticker: 'XME', name: 'Metals & Mining', color: '#fb923c' },
    { ticker: 'KRE', name: 'Regional Banks', color: '#c084fc' },
    { ticker: 'XBI', name: 'Biotech', color: '#f472b6' },
    { ticker: 'ITB', name: 'Home Construction', color: '#4ade80' },
    { ticker: 'IYT', name: 'Transportation', color: '#38bdf8' },
];

let selectedSector = null;
let sectorZScores = {};
let benchmarkPrices = null;
let isLoading = false;

const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const _cache = new Map();

try {
    const saved = localStorage.getItem('priceCache');
    if (saved) {
        const parsed = JSON.parse(saved);
        Object.entries(parsed).forEach(([k, v]) => {
            if (Date.now() - v.ts < CACHE_TTL_MS) {
                v.data = v.data.map(p => ({ ...p, date: new Date(p.date) }));
                _cache.set(k, v);
            }
        });
    }
} catch (e) {}

function saveCache() {
    try {
        const obj = {};
        _cache.forEach((v, k) => { obj[k] = v; });
        localStorage.setItem('priceCache', JSON.stringify(obj));
    } catch (e) {}
}

async function fetchWithRace(url, timeoutMs = 12000) {
    const proxies = [
        `https://api.allorigins.win/raw?url=${encodeURIComponent(url)}`,
        `https://corsproxy.io/?${encodeURIComponent(url)}`,
        `https://api.codetabs.com/v1/proxy?quest=${encodeURIComponent(url)}`,
    ];
    
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    
    try {
        const response = await Promise.any(
            proxies.map(async (proxyUrl) => {
                const res = await fetch(proxyUrl, { signal: controller.signal });
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                const text = await res.text();
                if (!text.trim().startsWith('{') && !text.trim().startsWith('[') && !text.trim().startsWith('Date,')) {
                    throw new Error('Invalid');
                }
                return text;
            })
        );
        clearTimeout(timeout);
        return response;
    } catch (e) {
        clearTimeout(timeout);
        throw new Error('All proxies failed');
    }
}

async function fetchYahoo(ticker) {
    const cacheKey = `y:${ticker}`;
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < CACHE_TTL_MS) return hit.data;

    const p2 = Math.floor(Date.now() / 1000);
    const p1 = p2 - Math.floor(25 * 365.25 * 24 * 60 * 60);
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?period1=${p1}&period2=${p2}&interval=1wk`;

    const text = await fetchWithRace(url);
    const data = JSON.parse(text);
    
    const result = data?.chart?.result?.[0];
    if (!result) throw new Error('No data');

    const ts = result.timestamp || [];
    const closes = result.indicators?.adjclose?.[0]?.adjclose || result.indicators?.quote?.[0]?.close || [];

    const prices = [];
    for (let i = 0; i < ts.length; i++) {
        if (closes[i] != null && closes[i] > 0) {
            prices.push({ date: new Date(ts[i] * 1000), close: closes[i] });
        }
    }

    _cache.set(cacheKey, { ts: Date.now(), data: prices });
    saveCache();
    return prices;
}

async function fetchStooq(ticker) {
    const cacheKey = `s:${ticker}`;
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < CACHE_TTL_MS) return hit.data;

    const sym = `${ticker.toLowerCase()}.us`;
    const url = `https://stooq.com/q/d/l/?s=${sym}&i=w`;
    const text = await fetchWithRace(url, 15000);
    
    if (!text.startsWith('Date,')) throw new Error('Invalid');

    const lines = text.split(/\r?\n/);
    lines.shift();
    
    const prices = [];
    for (const line of lines) {
        const cols = line.split(',');
        const close = Number(cols[4]);
        if (cols[0] && close > 0) prices.push({ date: new Date(cols[0]), close });
    }
    prices.sort((a, b) => a.date - b.date);

    _cache.set(cacheKey, { ts: Date.now(), data: prices });
    saveCache();
    return prices;
}

async function fetchPrices(ticker) {
    const yKey = `y:${ticker}`, sKey = `s:${ticker}`;
    if (_cache.has(yKey) && Date.now() - _cache.get(yKey).ts < CACHE_TTL_MS) return _cache.get(yKey).data;
    if (_cache.has(sKey) && Date.now() - _cache.get(sKey).ts < CACHE_TTL_MS) return _cache.get(sKey).data;
    try { return await fetchYahoo(ticker); } catch { return await fetchStooq(ticker); }
}

function calculateZScoreData(sectorPrices) {
    const retYears = parseInt(document.getElementById('returnPeriod').value);
    const zYears = parseInt(document.getElementById('zscoreWindow').value);
    const retWeeks = Math.round(retYears * 52);
    const zWeeks = Math.round(zYears * 52);
    
    const calcRet = (prices, weeks) => {
        const r = [];
        for (let i = weeks; i < prices.length; i++) {
            if (prices[i-weeks]?.close && prices[i]?.close) {
                r.push({ date: prices[i].date, value: ((prices[i].close / prices[i-weeks].close) - 1) * 100 });
            }
        }
        return r;
    };
    
    const sectorRet = calcRet(sectorPrices, retWeeks);
    const benchRet = calcRet(benchmarkPrices, retWeeks);
    
    const benchMap = new Map();
    benchRet.forEach(r => benchMap.set(`${r.date.getFullYear()}-${r.date.getMonth()}-${r.date.getDate()}`, r.value));
    
    const relRet = sectorRet.map(r => {
        const k = `${r.date.getFullYear()}-${r.date.getMonth()}-${r.date.getDate()}`;
        let bv = benchMap.get(k);
        if (bv === undefined) {
            for (let o = 1; o <= 7 && bv === undefined; o++) {
                const d = new Date(r.date); d.setDate(d.getDate() - o);
                bv = benchMap.get(`${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`);
            }
        }
        return bv !== undefined ? { date: r.date, value: r.value - bv } : null;
    }).filter(Boolean);
    
    const zscores = [];
    const minW = Math.min(zWeeks, Math.floor(relRet.length * 0.3));
    for (let i = minW; i < relRet.length; i++) {
        const win = relRet.slice(Math.max(0, i - zWeeks), i).map(r => r.value);
        if (win.length < 20) continue;
        const mean = win.reduce((a, b) => a + b, 0) / win.length;
        const std = Math.sqrt(win.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / win.length);
        if (std > 0.5) zscores.push({ date: relRet[i].date, value: Math.max(-6, Math.min(6, (relRet[i].value - mean) / std)) });
    }
    
    const monthly = new Map();
    zscores.forEach(d => monthly.set(`${d.date.getFullYear()}-${String(d.date.getMonth()+1).padStart(2,'0')}`, d));
    return Array.from(monthly.values()).sort((a, b) => a.date - b.date);
}

function normalizePrices(prices) {
    if (!prices?.length) return [];
    const start = prices[0].close;
    return prices.map(p => ({ date: p.date, value: ((p.close / start) - 1) * 100 }));
}

function setStatus(status, text) {
    document.getElementById('statusDot').className = 'status-dot' + (status === 'loading' ? ' loading' : status === 'error' ? ' error' : '');
    document.getElementById('statusText').textContent = text;
}

function selectSector(ticker) {
    selectedSector = ticker;
    renderSectorList();
    renderChart();
    localStorage.setItem('selectedSector', ticker);
}

function renderSectorList() {
    const sorted = [...SECTORS].sort((a, b) => {
        const aZ = sectorZScores[a.ticker]?.slice(-1)[0]?.value ?? 999;
        const bZ = sectorZScores[b.ticker]?.slice(-1)[0]?.value ?? 999;
        return aZ - bZ;
    });
    
    document.getElementById('sectorList').innerHTML = sorted.map(s => {
        const z = sectorZScores[s.ticker]?.slice(-1)[0]?.value;
        const sel = selectedSector === s.ticker;
        const sig = z === undefined ? '' : z < -2 ? 'cyclical-low' : z < -1 ? 'cheap' : z > 2 ? 'extended' : 'neutral';
        const sigTxt = z === undefined ? '' : z < -2 ? 'CYCLICAL LOW' : z < -1 ? 'CHEAP' : z > 2 ? 'EXTENDED' : 'NEUTRAL';
        const valCls = z === undefined ? '' : z < -1 ? 'negative' : z > 1 ? 'positive' : 'neutral';
        const valStr = z !== undefined ? `${z >= 0 ? '+' : ''}${z.toFixed(2)}` : '...';
        
        return `<div class="sector-row ${sel ? 'selected' : ''}" onclick="selectSector('${s.ticker}')">
            <div class="sector-info"><span class="dot" style="background:${s.color}"></span><span class="ticker">${s.ticker}</span><span class="name">${s.name}</span></div>
            <div class="sector-data"><span class="zscore ${valCls}">${valStr}</span>${sig ? `<span class="signal ${sig}">${sigTxt}</span>` : ''}</div>
        </div>`;
    }).join('');
}

function renderChart() {
    const container = document.getElementById('chartContainer');
    if (!selectedSector) { container.innerHTML = '<div class="empty">Select a sector</div>'; return; }
    
    const s = SECTORS.find(x => x.ticker === selectedSector) || { ticker: selectedSector, name: selectedSector, color: '#888' };
    const z = sectorZScores[selectedSector]?.slice(-1)[0]?.value;
    const valCls = z === undefined ? '' : z < -1 ? 'negative' : z > 1 ? 'positive' : 'neutral';
    const valStr = z !== undefined ? `${z >= 0 ? '+' : ''}${z.toFixed(2)}` : '...';
    const bench = document.getElementById('benchmark').value;
    
    container.innerHTML = `
        <div class="chart-head">
            <div class="title" style="color:${s.color}">${s.name} <span class="tk">${s.ticker}</span></div>
            <div class="zscore-display"><span class="label">Z-Score:</span><span class="val ${valCls}">${valStr}</span></div>
        </div>
        
        <div class="chart-section">
            <div class="chart-label">Cyclical Z-Score (vs ${bench})</div>
            <div class="chart-wrap zscore-chart"><canvas id="zscoreChart"></canvas></div>
        </div>
        
        <div class="chart-section">
            <div class="chart-label">Price Performance</div>
            <div class="legend"><span><i style="background:${s.color}"></i>${s.ticker}</span><span><i style="background:rgba(255,255,255,0.5)"></i>${bench}</span></div>
            <div class="chart-wrap price-chart"><canvas id="priceChart"></canvas></div>
        </div>
    `;
    
    createZScoreChart(selectedSector, s.color);
    createPriceChart(selectedSector, s.color, bench);
}

let zscoreChartInstance = null;
let priceChartInstance = null;

function createZScoreChart(ticker, color) {
    const canvas = document.getElementById('zscoreChart');
    if (!canvas) return;
    if (zscoreChartInstance) zscoreChartInstance.destroy();
    
    const zData = sectorZScores[ticker];
    if (!zData?.length) return;
    
    zscoreChartInstance = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            datasets: [{
                label: 'Z-Score',
                data: zData.map(d => ({ x: d.date, y: d.value })),
                borderColor: color,
                borderWidth: 2,
                pointRadius: 0,
                tension: 0.1,
                fill: false
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false, animation: false,
            plugins: { legend: { display: false }, tooltip: { callbacks: {
                title: ctx => ctx[0].raw.x.toLocaleDateString(),
                label: ctx => `Z-Score: ${ctx.parsed.y >= 0 ? '+' : ''}${ctx.parsed.y.toFixed(2)}`
            }}},
            scales: {
                x: { type: 'time', time: { unit: 'year' }, grid: { color: '#1a1a2e' }, ticks: { color: '#555', maxTicksLimit: 10 }},
                y: { min: -6, max: 6, grid: { color: '#1a1a2e' }, ticks: { color: '#555' }}
            }
        },
        plugins: [{
            id: 'refLines',
            beforeDraw: c => {
                const ctx = c.ctx, y = c.scales.y, x = c.scales.x;
                ctx.save();
                // Zero line
                ctx.strokeStyle = '#444'; ctx.lineWidth = 1;
                ctx.beginPath(); ctx.moveTo(x.left, y.getPixelForValue(0)); ctx.lineTo(x.right, y.getPixelForValue(0)); ctx.stroke();
                // Reference lines
                ctx.setLineDash([5, 5]);
                ctx.strokeStyle = '#ef4444'; // -2 line (cyclical low)
                ctx.beginPath(); ctx.moveTo(x.left, y.getPixelForValue(-2)); ctx.lineTo(x.right, y.getPixelForValue(-2)); ctx.stroke();
                ctx.strokeStyle = '#22c55e'; // +2 line (extended)
                ctx.beginPath(); ctx.moveTo(x.left, y.getPixelForValue(2)); ctx.lineTo(x.right, y.getPixelForValue(2)); ctx.stroke();
                ctx.restore();
            }
        }]
    });
}

function createPriceChart(ticker, color, bench) {
    const canvas = document.getElementById('priceChart');
    if (!canvas) return;
    if (priceChartInstance) priceChartInstance.destroy();
    
    const sCache = _cache.get(`y:${ticker}`) || _cache.get(`s:${ticker}`);
    const bCache = _cache.get(`y:${bench}`) || _cache.get(`s:${bench}`);
    if (!sCache?.data || !bCache?.data) return;
    
    const start = new Date(Math.max(sCache.data[0].date, bCache.data[0].date));
    const sNorm = normalizePrices(sCache.data.filter(p => p.date >= start));
    const bNorm = normalizePrices(bCache.data.filter(p => p.date >= start));
    
    priceChartInstance = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            datasets: [
                { label: ticker, data: sNorm.map(d => ({ x: d.date, y: d.value })), borderColor: color, borderWidth: 2, pointRadius: 0, tension: 0.1 },
                { label: bench, data: bNorm.map(d => ({ x: d.date, y: d.value })), borderColor: 'rgba(255,255,255,0.5)', borderWidth: 2, pointRadius: 0, tension: 0.1 }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false, animation: false,
            interaction: { intersect: false, mode: 'index' },
            plugins: { legend: { display: false }, tooltip: { callbacks: {
                title: ctx => ctx[0].raw.x.toLocaleDateString(),
                label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y >= 0 ? '+' : ''}${ctx.parsed.y.toFixed(1)}%`
            }}},
            scales: {
                x: { type: 'time', time: { unit: 'year' }, grid: { color: '#1a1a2e' }, ticks: { color: '#555', maxTicksLimit: 10 }},
                y: { grid: { color: '#1a1a2e' }, ticks: { color: '#555', callback: v => `${v >= 0 ? '+' : ''}${v}%` }}
            }
        },
        plugins: [{ id: 'zero', beforeDraw: c => {
            const ctx = c.ctx, y = c.scales.y, x = c.scales.x, zy = y.getPixelForValue(0);
            if (zy >= y.top && zy <= y.bottom) { ctx.save(); ctx.strokeStyle = '#444'; ctx.beginPath(); ctx.moveTo(x.left, zy); ctx.lineTo(x.right, zy); ctx.stroke(); ctx.restore(); }
        }}]
    });
}

async function refreshAllData() {
    if (isLoading) return;
    isLoading = true;
    
    const bench = document.getElementById('benchmark').value;
    setStatus('loading', `Loading ${bench}...`);
    
    try {
        benchmarkPrices = await fetchPrices(bench);
        setStatus('loading', 'Loading sectors...');
        
        const results = await Promise.allSettled(SECTORS.map(async s => {
            const prices = await fetchPrices(s.ticker);
            return { ticker: s.ticker, data: calculateZScoreData(prices) };
        }));
        
        results.forEach((r, i) => {
            sectorZScores[SECTORS[i].ticker] = r.status === 'fulfilled' ? r.value.data : [];
        });
        
        if (!selectedSector) {
            const best = [...SECTORS].sort((a, b) => (sectorZScores[a.ticker]?.slice(-1)[0]?.value ?? 999) - (sectorZScores[b.ticker]?.slice(-1)[0]?.value ?? 999))[0];
            selectedSector = localStorage.getItem('selectedSector') || best?.ticker || SECTORS[0].ticker;
        }
        
        renderSectorList();
        renderChart();
        setStatus('ready', 'Ready');
    } catch (e) {
        setStatus('error', e.message);
    }
    isLoading = false;
}

document.getElementById('returnPeriod').addEventListener('change', refreshAllData);
document.getElementById('zscoreWindow').addEventListener('change', refreshAllData);
document.getElementById('benchmark').addEventListener('change', refreshAllData);

selectedSector = localStorage.getItem('selectedSector');
refreshAllData();
