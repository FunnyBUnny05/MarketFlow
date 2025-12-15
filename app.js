// Sector Z-Score Dashboard v5 - Fixed
// Proper fetch handling, throttled concurrency, real ETF holdings

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

// SSGA ETFs with daily holdings XLSX
const SSGA_ETFS = new Set([
    'XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'XLRE', 'XLC',
    'XHB', 'XOP', 'XME', 'KRE', 'XBI'
]);

const FINNHUB_BASE_URL = 'https://finnhub.io/api/v1';
const FINNHUB_API_KEY_STORAGE_KEY = 'finnhubApiKey';
const DEFAULT_FINNHUB_API_KEY = 'd501pmhr01qsabpqjea0d501pmhr01qsabpqjeag';
let FINNHUB_API_KEY = localStorage.getItem(FINNHUB_API_KEY_STORAGE_KEY) || DEFAULT_FINNHUB_API_KEY;

let selectedSector = null;
let sectorZScores = {};
let sectorDataQuality = {}; // Track data quality per sector
let benchmarkPrices = null;
let isLoading = false;

const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const _cache = new Map();

// Load cache - store dates as ISO strings
try {
    const saved = localStorage.getItem('priceCache');
    if (saved) {
        const parsed = JSON.parse(saved);
        Object.entries(parsed).forEach(([k, v]) => {
            if (Date.now() - v.ts < CACHE_TTL_MS) {
                if (Array.isArray(v.data)) {
                    v.data = v.data.map(p => ({
                        ...p,
                        date: p.date ? new Date(p.date) : undefined
                    }));
                }
                _cache.set(k, v);
            }
        });
    }
} catch (e) { console.log('Cache load error:', e); }

function saveCache() {
    try {
        const obj = {};
        _cache.forEach((v, k) => {
            obj[k] = {
                ts: v.ts,
                data: Array.isArray(v.data) ? v.data.map(p => ({
                    ...p,
                    date: p.date instanceof Date ? p.date.toISOString() : p.date
                })) : v.data
            };
        });
        localStorage.setItem('priceCache', JSON.stringify(obj));
    } catch (e) { console.log('Cache save error:', e); }
}

// ============ FETCH UTILITIES ============

function requireFinnhubApiKey() {
    if (!FINNHUB_API_KEY) {
        const entered = prompt('Enter Finnhub API key');
        if (entered) {
            FINNHUB_API_KEY = entered.trim();
            localStorage.setItem(FINNHUB_API_KEY_STORAGE_KEY, FINNHUB_API_KEY);
        }
    }

    if (!FINNHUB_API_KEY) {
        throw new Error('Missing Finnhub API key');
    }

    return FINNHUB_API_KEY;
}

async function fetchFromFinnhub(path, params = {}) {
    const token = requireFinnhubApiKey();
    const url = `${FINNHUB_BASE_URL}${path}?${new URLSearchParams({ ...params, token })}`;
    const res = await fetch(url);
    if (!res.ok) {
        throw new Error(`Finnhub error ${res.status}`);
    }
    const data = await res.json();
    if (data?.error) {
        throw new Error(data.error);
    }
    return data;
}

// Throttled parallel execution - prevents overload
async function mapLimit(items, limit, fn) {
    const results = new Array(items.length);
    let idx = 0;

    const worker = async () => {
        while (idx < items.length) {
            const i = idx++;
            try {
                results[i] = { status: 'fulfilled', value: await fn(items[i], i) };
            } catch (e) {
                results[i] = { status: 'rejected', reason: e };
            }
        }
    };

    await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
    return results;
}

// ============ CORS PROXY UTILITIES (for Yahoo/Stooq) ============

const proxyHealth = {
    'allorigins': { failures: 0, disabledUntil: 0 },
    'corsproxy': { failures: 0, disabledUntil: 0 },
    'codetabs': { failures: 0, disabledUntil: 0 }
};
const CIRCUIT_BREAKER_THRESHOLD = 3;
const CIRCUIT_BREAKER_COOLDOWN_MS = 10 * 60 * 1000;

function getActiveProxies(url) {
    const now = Date.now();
    const allProxies = [
        { name: 'allorigins', url: `https://api.allorigins.win/raw?url=${encodeURIComponent(url)}` },
        { name: 'corsproxy', url: `https://corsproxy.io/?${encodeURIComponent(url)}` },
        { name: 'codetabs', url: `https://api.codetabs.com/v1/proxy?quest=${encodeURIComponent(url)}` },
    ];

    return allProxies.filter(p => {
        const health = proxyHealth[p.name];
        if (health.disabledUntil > now) return false;
        if (health.failures >= CIRCUIT_BREAKER_THRESHOLD) health.failures = 0;
        return true;
    });
}

function recordProxySuccess(proxyName) {
    if (proxyHealth[proxyName]) {
        proxyHealth[proxyName].failures = 0;
        proxyHealth[proxyName].disabledUntil = 0;
    }
}

function recordProxyFailure(proxyName) {
    if (proxyHealth[proxyName]) {
        proxyHealth[proxyName].failures++;
        if (proxyHealth[proxyName].failures >= CIRCUIT_BREAKER_THRESHOLD) {
            proxyHealth[proxyName].disabledUntil = Date.now() + CIRCUIT_BREAKER_COOLDOWN_MS;
        }
    }
}

async function fetchWithProxy(url, timeoutMs = 12000) {
    const proxies = getActiveProxies(url);

    if (proxies.length === 0) {
        Object.values(proxyHealth).forEach(h => { h.failures = 0; h.disabledUntil = 0; });
        return fetchWithProxy(url, timeoutMs);
    }

    const controllers = proxies.map(() => new AbortController());
    const timeout = setTimeout(() => controllers.forEach(c => c.abort()), timeoutMs);

    try {
        const wrapped = proxies.map((p, i) => (async () => {
            try {
                const res = await fetch(p.url, { signal: controllers[i].signal });
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                const text = await res.text();
                const t = text.trim();
                if (!(t.startsWith('{') || t.startsWith('[') || t.startsWith('Date,'))) {
                    throw new Error('Invalid response');
                }
                recordProxySuccess(p.name);
                return { text, proxyName: p.name };
            } catch (e) {
                recordProxyFailure(p.name);
                throw e;
            }
        })());

        const result = await new Promise((resolve, reject) => {
            let pending = wrapped.length, lastErr;
            wrapped.forEach(p =>
                p.then(resolve).catch(err => {
                    lastErr = err;
                    pending--;
                    if (pending === 0) reject(lastErr);
                })
            );
        });

        controllers.forEach(c => c.abort());
        return result.text;
    } finally {
        clearTimeout(timeout);
    }
}

// Binary fetch for XLSX files
async function fetchBinaryWithProxy(url, timeoutMs = 15000) {
    const proxies = getActiveProxies(url);

    if (proxies.length === 0) {
        Object.values(proxyHealth).forEach(h => { h.failures = 0; h.disabledUntil = 0; });
        return fetchBinaryWithProxy(url, timeoutMs);
    }

    const controllers = proxies.map(() => new AbortController());
    const timeout = setTimeout(() => controllers.forEach(c => c.abort()), timeoutMs);

    try {
        const result = await new Promise((resolve, reject) => {
            let pending = proxies.length, lastErr;
            proxies.forEach((p, i) => {
                fetch(p.url, { signal: controllers[i].signal })
                    .then(r => {
                        if (!r.ok) throw new Error(`HTTP ${r.status}`);
                        return r.arrayBuffer();
                    })
                    .then(buf => {
                        const b = new Uint8Array(buf);
                        if (b.length < 4 || b[0] !== 0x50 || b[1] !== 0x4B) {
                            throw new Error('Not XLSX');
                        }
                        recordProxySuccess(p.name);
                        resolve(buf);
                    })
                    .catch(err => {
                        recordProxyFailure(p.name);
                        lastErr = err;
                        pending--;
                        if (pending === 0) reject(lastErr);
                    });
            });
        });

        controllers.forEach(c => c.abort());
        return result;
    } finally {
        clearTimeout(timeout);
    }
}

// ============ ETF PRICE DATA FETCHING (Yahoo/Stooq) ============

async function fetchYahoo(ticker) {
    const cacheKey = `y:${ticker}`;
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < CACHE_TTL_MS) return { prices: hit.data, source: 'Yahoo', fromCache: true };

    const p2 = Math.floor(Date.now() / 1000);
    const p1 = p2 - Math.floor(25 * 365.25 * 24 * 60 * 60);
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?period1=${p1}&period2=${p2}&interval=1wk`;

    const text = await fetchWithProxy(url);
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
    return { prices, source: 'Yahoo', fromCache: false };
}

async function fetchStooq(ticker) {
    const cacheKey = `s:${ticker}`;
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < CACHE_TTL_MS) return { prices: hit.data, source: 'Stooq', fromCache: true };

    const sym = `${ticker.toLowerCase()}.us`;
    const url = `https://stooq.com/q/d/l/?s=${sym}&i=w`;
    const text = await fetchWithProxy(url, 15000);

    if (!text.startsWith('Date,')) throw new Error('Invalid Stooq');

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
    return { prices, source: 'Stooq', fromCache: false };
}

// Fetch ETF prices - Yahoo first, Stooq fallback
async function fetchPrices(ticker) {
    const yKey = `y:${ticker}`, sKey = `s:${ticker}`;
    const yHit = _cache.get(yKey);
    const sHit = _cache.get(sKey);

    if (yHit && Date.now() - yHit.ts < CACHE_TTL_MS) {
        return { prices: yHit.data, source: 'Yahoo', fromCache: true };
    }
    if (sHit && Date.now() - sHit.ts < CACHE_TTL_MS) {
        return { prices: sHit.data, source: 'Stooq', fromCache: true };
    }

    try {
        return await fetchYahoo(ticker);
    } catch {
        return await fetchStooq(ticker);
    }
}

// Fetch individual stock prices from Finnhub (for co-move calculation)
async function fetchStockPrices(ticker) {
    const cacheKey = `fh:${ticker}`;
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < CACHE_TTL_MS) {
        return { prices: hit.data, source: 'Finnhub', fromCache: true };
    }

    const to = Math.floor(Date.now() / 1000);
    const from = to - Math.floor(25 * 365.25 * 24 * 60 * 60);

    const data = await fetchFromFinnhub('/stock/candle', {
        symbol: ticker,
        resolution: 'W',
        from,
        to
    });

    if (data?.s !== 'ok' || !Array.isArray(data?.c) || !Array.isArray(data?.t)) {
        throw new Error('Invalid Finnhub candle response');
    }

    const prices = data.t.map((ts, i) => ({
        date: new Date(ts * 1000),
        close: data.c[i]
    })).filter(p => p.close != null && p.close > 0);

    _cache.set(cacheKey, { ts: Date.now(), data: prices });
    saveCache();
    return { prices, source: 'Finnhub', fromCache: false };
}

// ============ Z-SCORE CALCULATIONS ============

function calcRollingReturnPct(prices, weeks) {
    const out = [];
    for (let i = weeks; i < prices.length; i++) {
        const a = prices[i - weeks]?.close;
        const b = prices[i]?.close;
        if (a && b && a > 0 && b > 0) {
            out.push({ date: prices[i].date, value: ((b / a) - 1) * 100 });
        }
    }
    return out;
}

// Simple week-over-week returns
function calcWeeklyReturns(prices) {
    const out = [];
    for (let i = 1; i < prices.length; i++) {
        const prev = prices[i - 1];
        const cur = prices[i];
        if (prev?.close > 0 && cur?.close > 0) {
            out.push({ date: cur.date, value: ((cur.close / prev.close) - 1) * 100 });
        }
    }
    return out;
}

function calcReturnStats(values) {
    if (!values.length) return { mean: 0, variance: 0, std: 0, count: 0 };
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const denom = Math.max(1, values.length - 1);
    const variance = values.reduce((a, b) => a + (b - mean) * (b - mean), 0) / denom;
    return { mean, variance, std: Math.sqrt(variance), count: values.length };
}

// Build benchmark index for fast nearest-date lookup
function buildReturnIndex(retSeries) {
    const times = retSeries.map(r => r.date.getTime());
    const values = retSeries.map(r => r.value);
    return { times, values };
}

// Binary search nearest neighbor lookup
function nearestValue(index, ts, maxDeltaDays = 10) {
    const { times, values } = index;
    if (!times.length) return null;

    let lo = 0, hi = times.length - 1;
    while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (times[mid] < ts) lo = mid + 1;
        else hi = mid;
    }

    const DAY_MS = 86400000;
    const maxDelta = maxDeltaDays * DAY_MS;

    let bestIdx = lo;
    let bestDist = Math.abs(times[lo] - ts);

    if (lo > 0) {
        const d2 = Math.abs(times[lo - 1] - ts);
        if (d2 < bestDist) { bestDist = d2; bestIdx = lo - 1; }
    }

    if (bestDist > maxDelta) return null;
    return values[bestIdx];
}

function calculateZScoreData(sectorPrices, benchIndex, source) {
    const retYears = parseInt(document.getElementById('returnPeriod')?.value || '3');
    const zYears = parseInt(document.getElementById('zscoreWindow')?.value || '10');
    const retWeeks = Math.round(retYears * 52);
    const zWeeks = Math.round(zYears * 52);
    
    const sectorRet = calcRollingReturnPct(sectorPrices, retWeeks);
    
    // Align to benchmark by nearest weekly point (±10 days for weekly + holidays)
    let alignedCount = 0;
    let missedCount = 0;
    
    const relRet = sectorRet.map(r => {
        const bv = nearestValue(benchIndex, r.date.getTime(), 10);
        if (bv == null) {
            missedCount++;
            return null;
        }
        alignedCount++;
        return { date: r.date, value: r.value - bv };
    }).filter(Boolean);
    
    const zscores = [];
    const minWindow = Math.min(zWeeks, relRet.length);
    const warmup = Math.max(52, Math.floor(minWindow * 0.6)); // require real history
    
    for (let i = warmup; i < relRet.length; i++) {
        const start = Math.max(0, i - zWeeks);
        const win = relRet.slice(start, i).map(r => r.value);
        if (win.length < 30) continue;
        
        const mean = win.reduce((a, b) => a + b, 0) / win.length;
        
        // Sample std (N-1) is slightly better behaved
        const denom = Math.max(1, win.length - 1);
        const variance = win.reduce((a, b) => a + (b - mean) * (b - mean), 0) / denom;
        const std = Math.sqrt(variance);
        
        if (std < 1e-6) continue; // truly flat, not just "low vol"
        const z = (relRet[i].value - mean) / std;
        
        zscores.push({ date: relRet[i].date, value: Math.max(-6, Math.min(6, z)) });
    }
    
    // Dedupe monthly (keep last point in month)
    const monthly = new Map();
    zscores.forEach(d => {
        const k = `${d.date.getFullYear()}-${String(d.date.getMonth() + 1).padStart(2, '0')}`;
        monthly.set(k, d);
    });
    
    const zscore = Array.from(monthly.values()).sort((a, b) => a.date - b.date);
    
    // Data quality metrics
    const quality = {
        source: source || 'Unknown',
        pointCount: sectorPrices.length,
        startDate: sectorPrices[0]?.date,
        endDate: sectorPrices[sectorPrices.length - 1]?.date,
        alignedCount,
        missedCount,
        alignmentPct: alignedCount > 0 ? Math.round((alignedCount / (alignedCount + missedCount)) * 100) : 0,
        zscoreCount: zscore.length
    };
    
    return { zscore, quality };
}

function normalizePrices(prices) {
    if (!prices?.length) return [];
    const start = prices[0].close;
    return prices.map(p => ({ date: p.date, value: ((p.close / start) - 1) * 100 }));
}

// ============ HOLDINGS DATA (SSGA XLSX) ============

async function fetchSSGAHoldings(etfTicker) {
    const cacheKey = `holdings:${etfTicker}`;
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < CACHE_TTL_MS) return hit.data;

    const url = `https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-${etfTicker.toLowerCase()}.xlsx`;

    try {
        const buf = await fetchBinaryWithProxy(url, 20000);
        const wb = XLSX.read(buf, { type: 'array' });
        const ws = wb.Sheets[wb.SheetNames[0]];
        const rows = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true });

        let headerIdx = rows.findIndex(r => r && r.some(c => String(c).trim().toLowerCase() === 'ticker'));
        if (headerIdx < 0) return [];

        const header = rows[headerIdx].map(x => String(x || '').trim().toLowerCase());
        const tickerCol = header.findIndex(h => h === 'ticker');
        const weightCol = header.findIndex(h => h.includes('weight'));
        const nameCol = header.findIndex(h => h === 'name' || h === 'security name');

        if (tickerCol < 0) return [];

        const holdings = [];
        for (let i = headerIdx + 1; i < rows.length; i++) {
            const t = String(rows[i]?.[tickerCol] || '').trim().toUpperCase();
            if (!t || t === '-' || t.length > 10) continue;
            if (t.includes(' ') || t.includes('/')) continue;

            let weight = 0;
            if (weightCol >= 0) {
                const w = rows[i]?.[weightCol];
                weight = typeof w === 'number' ? w : parseFloat(String(w).replace('%', '')) || 0;
            }

            const name = nameCol >= 0 ? String(rows[i]?.[nameCol] || '').trim() : t;
            holdings.push({ ticker: t, name, weight });
        }

        holdings.sort((a, b) => b.weight - a.weight);
        const seen = new Set();
        const unique = holdings.filter(h => {
            if (seen.has(h.ticker)) return false;
            seen.add(h.ticker);
            return true;
        }).slice(0, 100);

        _cache.set(cacheKey, { ts: Date.now(), data: unique });
        saveCache();
        return unique;
    } catch (e) {
        console.log(`SSGA holdings fetch failed for ${etfTicker}:`, e.message);
        return [];
    }
}

async function fetchFinnhubQuote(symbol) {
    const cacheKey = `quote:${symbol}`;
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < 5 * 60 * 1000) return hit.data; // 5 minute cache

    const data = await fetchFromFinnhub('/quote', { symbol });
    const quote = {
        symbol,
        price: data?.c ?? 0,
        change: data?.dp ?? 0,
        volume: data?.v ?? 0
    };

    _cache.set(cacheKey, { ts: Date.now(), data: quote });
    return quote;
}

async function calculateCoMoveScore(symbol, sectorIndex, sectorStats) {
    try {
        const { prices } = await fetchStockPrices(symbol);
        const stockReturns = calcWeeklyReturns(prices.slice(-110));

        const pairs = [];
        for (const r of stockReturns) {
            const sv = nearestValue(sectorIndex, r.date.getTime(), 7);
            if (sv == null) continue;
            pairs.push({ stock: r.value, sector: sv });
        }

        if (pairs.length < 8 || sectorStats.std < 1e-6 || sectorStats.variance < 1e-6) return null;

        const stockVals = pairs.map(p => p.stock);
        const sectorVals = pairs.map(p => p.sector);
        const stockStats = calcReturnStats(stockVals);
        const sectorMean = sectorVals.reduce((a, b) => a + b, 0) / sectorVals.length;

        if (stockStats.std < 1e-6) return null;

        let cov = 0;
        for (let i = 0; i < pairs.length; i++) {
            cov += (stockVals[i] - stockStats.mean) * (sectorVals[i] - sectorMean);
        }
        cov /= Math.max(1, pairs.length - 1);

        const corr = cov / (stockStats.std * sectorStats.std);
        const beta = cov / sectorStats.variance;

        if (!isFinite(corr) || !isFinite(beta) || corr <= 0 || beta <= 0) return null;

        // Higher score = stronger positive correlation * sensitivity to sector moves
        return corr * beta;
    } catch (e) {
        console.log(`Co-move calc failed for ${symbol}:`, e.message);
        return null;
    }
}

async function fetchHoldingsData(sectorTicker) {
    const cacheKey = `topstocks:${sectorTicker}`;
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < 5 * 60 * 1000) return hit.data; // 5 min cache

    // Only fetch holdings for SSGA ETFs
    if (!SSGA_ETFS.has(sectorTicker)) return [];

    const holdings = await fetchSSGAHoldings(sectorTicker);

    if (!holdings.length) return [];

    const quoteResults = await mapLimit(holdings.slice(0, 50), 5, async (h) => ({
        ...h,
        ...(await fetchFinnhubQuote(h.ticker))
    }));

    let results = quoteResults
        .filter(r => r.status === 'fulfilled' && r.value)
        .map(r => ({
            symbol: r.value.symbol,
            name: (r.value.name || r.value.symbol || '').substring(0, 28) || r.value.symbol,
            price: r.value.price,
            change: r.value.change,
            volume: r.value.volume,
            weight: r.value.weight || 0
        }));

    // Sort by weight, fallback to volume when weights are missing
    results.sort((a, b) => {
        if (a.weight === b.weight) return (b.volume || 0) - (a.volume || 0);
        return (b.weight || 0) - (a.weight || 0);
    });

    // Calculate which holdings move most with the sector (beta * correlation)
    try {
        const sectorPriceData = await fetchPrices(sectorTicker);
        const sectorReturns = calcWeeklyReturns(sectorPriceData.prices.slice(-110));
        const sectorStats = calcReturnStats(sectorReturns.map(r => r.value));

        if (sectorStats.count >= 10 && sectorStats.variance > 1e-6) {
            const sectorIndex = buildReturnIndex(sectorReturns);
            const scored = await mapLimit(results.slice(0, 15), 3, async r => ({
                ...r,
                coMoveScore: await calculateCoMoveScore(r.symbol, sectorIndex, sectorStats)
            }));

            const scoredValues = scored
                .filter(s => s.status === 'fulfilled' && s.value)
                .map(s => s.value);

            if (scoredValues.some(s => s.coMoveScore != null)) {
                results = scoredValues.sort((a, b) => (b.coMoveScore ?? -Infinity) - (a.coMoveScore ?? -Infinity));
            }
        }
    } catch (e) {
        console.log('Co-move scoring skipped:', e.message);
    }

    results = results.slice(0, 10);

    if (results.length > 0) {
        _cache.set(cacheKey, { ts: Date.now(), data: results });
    }
    
    return results;
}

// ============ UI FUNCTIONS ============

function setStatus(status, text) {
    const dot = document.getElementById('statusDot');
    const txt = document.getElementById('statusText');
    if (dot) dot.className = 'status-dot' + (status === 'loading' ? ' loading' : status === 'error' ? ' error' : '');
    if (txt) txt.textContent = text;
}

function selectSector(ticker) {
    selectedSector = ticker;
    renderSectorList();
    renderChart();
    localStorage.setItem('selectedSector', ticker);
}

function renderSectorList() {
    const container = document.getElementById('sectorList');
    if (!container) return;
    
    const sorted = [...SECTORS].sort((a, b) => {
        const aZ = sectorZScores[a.ticker]?.slice(-1)[0]?.value ?? 999;
        const bZ = sectorZScores[b.ticker]?.slice(-1)[0]?.value ?? 999;
        return aZ - bZ;
    });
    
    container.innerHTML = sorted.map(s => {
        const z = sectorZScores[s.ticker]?.slice(-1)[0]?.value;
        const sel = selectedSector === s.ticker;
        const sig = z === undefined ? '' : z < -2 ? 'extreme-weak' : z < -1 ? 'weak' : z > 2 ? 'extreme-strong' : 'neutral';
        const sigTxt = z === undefined ? '' : z < -2 ? 'EXTREME WEAK' : z < -1 ? 'WEAK' : z > 2 ? 'EXTREME STRONG' : 'NEUTRAL';
        const valCls = z === undefined ? '' : z < -1 ? 'negative' : z > 1 ? 'positive' : 'neutral';
        const valStr = z !== undefined ? `${z >= 0 ? '+' : ''}${z.toFixed(2)}` : '...';
        
        return `<div class="sector-row ${sel ? 'selected' : ''}" onclick="selectSector('${s.ticker}')">
            <div class="sector-info"><span class="dot" style="background:${s.color}"></span><span class="ticker">${s.ticker}</span><span class="name">${s.name}</span></div>
            <div class="sector-data"><span class="zscore ${valCls}">${valStr}</span>${sig ? `<span class="signal ${sig}">${sigTxt}</span>` : ''}</div>
        </div>`;
    }).join('');
}

// Calculate simple moving average
function calcSMA(prices, period) {
    if (prices.length < period) return null;
    const slice = prices.slice(-period);
    return slice.reduce((a, b) => a + b.close, 0) / period;
}

// Calculate rotation trigger: Z-score setup + trend confirmation
function calcRotationTrigger(sectorTicker, benchTicker) {
    const sCache = _cache.get(getPriceCacheKey(sectorTicker));
    const bCache = _cache.get(getPriceCacheKey(benchTicker));
    
    if (!sCache?.data || !bCache?.data) return null;
    
    const sectorPrices = sCache.data;
    const benchPrices = bCache.data;
    
    // Calculate relative strength (sector/benchmark ratio)
    const minLen = Math.min(sectorPrices.length, benchPrices.length);
    if (minLen < 30) return null;
    
    // Align by date - simple approach for weekly data
    const relStrength = [];
    let bIdx = 0;
    for (let i = 0; i < sectorPrices.length; i++) {
        const sDate = sectorPrices[i].date.getTime();
        // Find nearest benchmark date
        while (bIdx < benchPrices.length - 1 && benchPrices[bIdx + 1].date.getTime() <= sDate) {
            bIdx++;
        }
        if (Math.abs(benchPrices[bIdx].date.getTime() - sDate) < 10 * 86400000) {
            relStrength.push({
                date: sectorPrices[i].date,
                close: sectorPrices[i].close / benchPrices[bIdx].close
            });
        }
    }
    
    if (relStrength.length < 30) return null;
    
    // Calculate 30-week MA of relative strength (approx 150 trading days)
    const ma30 = calcSMA(relStrength, 30);
    const currentRS = relStrength[relStrength.length - 1].close;
    const prevRS = relStrength[relStrength.length - 5]?.close; // ~1 month ago
    
    // Get current Z-score
    const zData = sectorZScores[sectorTicker];
    const currentZ = zData?.slice(-1)[0]?.value;
    
    if (currentZ === undefined || ma30 === null) return null;
    
    // Determine setup and trigger
    const setup = currentZ < -1 ? 'WEAK' : currentZ > 1 ? 'STRONG' : 'NEUTRAL';
    const aboveMA = currentRS > ma30;
    const trending = prevRS ? (currentRS > prevRS ? 'UP' : 'DOWN') : 'FLAT';
    
    // Trigger logic
    let trigger = 'WAIT';
    let confidence = 0;
    
    if (setup === 'WEAK' && aboveMA && trending === 'UP') {
        trigger = 'BUY ROTATION';
        confidence = Math.min(100, Math.round(Math.abs(currentZ) * 30 + (aboveMA ? 20 : 0)));
    } else if (setup === 'STRONG' && !aboveMA && trending === 'DOWN') {
        trigger = 'SELL ROTATION';
        confidence = Math.min(100, Math.round(Math.abs(currentZ) * 30 + (!aboveMA ? 20 : 0)));
    } else if (setup === 'WEAK') {
        trigger = 'WATCH (weak, wait for trend)';
        confidence = 30;
    } else if (setup === 'STRONG') {
        trigger = 'CAUTION (extended)';
        confidence = 30;
    }
    
    return {
        setup,
        aboveMA,
        trending,
        trigger,
        confidence,
        currentZ: currentZ.toFixed(2),
        rsVsMa: ((currentRS / ma30 - 1) * 100).toFixed(1)
    };
}

function renderChart() {
    const container = document.getElementById('chartContainer');
    if (!container) return;
    if (!selectedSector) { container.innerHTML = '<div class="empty">Select a sector</div>'; return; }
    
    const s = SECTORS.find(x => x.ticker === selectedSector) || { ticker: selectedSector, name: selectedSector, color: '#888' };
    const z = sectorZScores[selectedSector]?.slice(-1)[0]?.value;
    const valCls = z === undefined ? '' : z < -1 ? 'negative' : z > 1 ? 'positive' : 'neutral';
    const valStr = z !== undefined ? `${z >= 0 ? '+' : ''}${z.toFixed(2)}` : '...';
    const bench = document.getElementById('benchmark')?.value || 'SPY';
    
    // Get data quality
    const quality = sectorDataQuality[selectedSector] || {};
    const qualityWarning = quality.alignmentPct < 90 ? 'warning' : quality.alignmentPct < 95 ? 'caution' : 'good';
    
    // Get rotation trigger
    const rotation = calcRotationTrigger(selectedSector, bench);
    
    container.innerHTML = `
        <div class="chart-head">
            <div class="title" style="color:${s.color}">${s.name} <span class="tk">${s.ticker}</span></div>
            <div class="zscore-display"><span class="label">Z-Score:</span><span class="val ${valCls}">${valStr}</span></div>
        </div>

        <div class="info-panels">
            <div class="panel data-quality-new ${qualityWarning}">
                <div class="panel-header">
                    <div class="panel-title-new">📊 Data Quality</div>
                    <div class="quality-badge ${qualityWarning}">
                        ${qualityWarning === 'good' ? '✓ Excellent' : qualityWarning === 'caution' ? '⚠ Fair' : '⚠ Low'}
                    </div>
                </div>

                <div class="quality-metrics">
                    <div class="metric-group">
                        <div class="metric-label">Data Source</div>
                        <div class="metric-value">${quality.source || 'N/A'}</div>
                    </div>

                    <div class="metric-group">
                        <div class="metric-label">Coverage</div>
                        <div class="metric-value">${quality.pointCount || 0} weeks</div>
                        <div class="metric-sublabel">${quality.startDate ? quality.startDate.toLocaleDateString('en-US', {month: 'short', year: 'numeric'}) : '?'} to ${quality.endDate ? quality.endDate.toLocaleDateString('en-US', {month: 'short', year: 'numeric'}) : '?'}</div>
                    </div>

                    <div class="metric-group highlight">
                        <div class="metric-label">Alignment</div>
                        <div class="metric-value-large ${qualityWarning}">${quality.alignmentPct || 0}%</div>
                        <div class="metric-sublabel">${quality.missedCount || 0} points misaligned</div>
                    </div>
                </div>
            </div>

            ${rotation ? `
            <div class="panel rotation-trigger-new">
                <div class="panel-header">
                    <div class="panel-title-new">🎯 Market Signal</div>
                    <div class="setup-badge ${rotation.setup.toLowerCase()}">${rotation.setup}</div>
                </div>

                <div class="signal-main">
                    <div class="trigger-action ${rotation.trigger.includes('BUY') ? 'buy' : rotation.trigger.includes('SELL') ? 'sell' : 'wait'}">
                        ${rotation.trigger}
                    </div>
                </div>

                <div class="rotation-metrics">
                    <div class="rotation-row">
                        <div class="rotation-label">Relative Strength</div>
                        <div class="rotation-value ${rotation.aboveMA ? 'positive' : 'negative'}">
                            ${rotation.aboveMA ? '↑' : '↓'} ${rotation.aboveMA ? 'Above' : 'Below'} MA
                            <span class="rotation-pct">(${rotation.rsVsMa > 0 ? '+' : ''}${rotation.rsVsMa}%)</span>
                        </div>
                    </div>

                    <div class="rotation-row">
                        <div class="rotation-label">Trend</div>
                        <div class="rotation-value ${rotation.trending === 'UP' ? 'positive' : rotation.trending === 'DOWN' ? 'negative' : ''}">
                            ${rotation.trending === 'UP' ? '📈 Trending Up' : rotation.trending === 'DOWN' ? '📉 Trending Down' : '➡️ Flat'}
                        </div>
                    </div>
                </div>
            </div>
            ` : ''}
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
        
        <div class="chart-section">
            <div class="chart-label">Top Holdings</div>
            <div id="holdingsTable" class="holdings-loading">Loading holdings...</div>
        </div>
    `;
    
    createZScoreChart(selectedSector, s.color);
    createPriceChart(selectedSector, s.color, bench);
    loadHoldings(selectedSector);
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
                title: ctx => ctx[0]?.raw?.x?.toLocaleDateString() || '',
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
                ctx.strokeStyle = '#444'; ctx.lineWidth = 1;
                ctx.beginPath(); ctx.moveTo(x.left, y.getPixelForValue(0)); ctx.lineTo(x.right, y.getPixelForValue(0)); ctx.stroke();
                ctx.setLineDash([5, 5]);
                ctx.strokeStyle = '#ef4444';
                ctx.beginPath(); ctx.moveTo(x.left, y.getPixelForValue(-2)); ctx.lineTo(x.right, y.getPixelForValue(-2)); ctx.stroke();
                ctx.strokeStyle = '#22c55e';
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
    
    const sCache = _cache.get(getPriceCacheKey(ticker));
    const bCache = _cache.get(getPriceCacheKey(bench));
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
                title: ctx => ctx[0]?.raw?.x?.toLocaleDateString() || '',
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

async function loadHoldings(sectorTicker) {
    const container = document.getElementById('holdingsTable');
    if (!container) return;
    
    try {
        const holdings = await fetchHoldingsData(sectorTicker);
        
        if (!holdings.length) {
            container.innerHTML = '<div class="holdings-empty">No holdings data available</div>';
            return;
        }

        // Check if we have weight data
        const hasWeights = holdings[0].weight > 0;
        const hasScores = holdings.some(h => h.coMoveScore != null);

        container.innerHTML = `
            <table class="holdings">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Symbol</th>
                        <th>Name</th>
                        <th>Price</th>
                        <th>Change</th>
                        ${hasScores ? '<th>Co-Move</th>' : ''}
                        <th>${hasWeights ? 'Weight' : 'Volume'}</th>
                    </tr>
                </thead>
                <tbody>
                    ${holdings.map((h, i) => `
                        <tr>
                            <td class="rank">${i + 1}</td>
                            <td class="symbol">${h.symbol}</td>
                            <td class="name">${h.name}</td>
                            <td class="price">$${h.price.toFixed(2)}</td>
                            <td class="change ${h.change >= 0 ? 'positive' : 'negative'}">${h.change >= 0 ? '+' : ''}${h.change.toFixed(2)}%</td>
                            ${hasScores ? `<td class="volume">${h.coMoveScore != null ? h.coMoveScore.toFixed(2) : '-'}</td>` : ''}
                            <td class="volume">${hasWeights ? h.weight.toFixed(2) + '%' : formatVolume(h.volume)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;
    } catch (e) {
        container.innerHTML = '<div class="holdings-empty">Failed to load holdings</div>';
    }
}

function formatVolume(vol) {
    if (vol >= 1e9) return (vol / 1e9).toFixed(1) + 'B';
    if (vol >= 1e6) return (vol / 1e6).toFixed(1) + 'M';
    if (vol >= 1e3) return (vol / 1e3).toFixed(1) + 'K';
    return vol.toString();
}

// ============ MAIN DATA REFRESH ============

async function refreshAllData() {
    if (isLoading) return;
    isLoading = true;
    
    const bench = document.getElementById('benchmark')?.value || 'SPY';
    const retYears = parseInt(document.getElementById('returnPeriod')?.value || '3');
    const retWeeks = Math.round(retYears * 52);
    
    setStatus('loading', `Loading ${bench}...`);
    
    try {
        const benchResult = await fetchPrices(bench);
        benchmarkPrices = benchResult.prices;
        
        // Precompute benchmark rolling returns ONCE
        const benchRet = calcRollingReturnPct(benchmarkPrices, retWeeks);
        const benchIndex = buildReturnIndex(benchRet);
        
        setStatus('loading', 'Loading sectors (0/' + SECTORS.length + ')...');
        
        // Throttled fetch - 5 concurrent max
        let completed = 0;
        const results = await mapLimit(SECTORS, 5, async (s) => {
            const priceResult = await fetchPrices(s.ticker);
            completed++;
            setStatus('loading', `Loading sectors (${completed}/${SECTORS.length})...`);
            const { zscore, quality } = calculateZScoreData(priceResult.prices, benchIndex, priceResult.source);
            return { ticker: s.ticker, zscore, quality };
        });
        
        results.forEach((r, i) => {
            if (r.status === 'fulfilled') {
                sectorZScores[SECTORS[i].ticker] = r.value.zscore;
                sectorDataQuality[SECTORS[i].ticker] = r.value.quality;
            } else {
                sectorZScores[SECTORS[i].ticker] = [];
                sectorDataQuality[SECTORS[i].ticker] = { source: 'Error', pointCount: 0 };
            }
        });
        
        if (!selectedSector) {
            const best = [...SECTORS].sort((a, b) => 
                (sectorZScores[a.ticker]?.slice(-1)[0]?.value ?? 999) - 
                (sectorZScores[b.ticker]?.slice(-1)[0]?.value ?? 999)
            )[0];
            selectedSector = localStorage.getItem('selectedSector') || best?.ticker || SECTORS[0].ticker;
        }
        
        renderSectorList();
        renderChart();
        setStatus('ready', 'Ready');
    } catch (e) {
        console.error('Refresh error:', e);
        setStatus('error', e.message);
    }
    isLoading = false;
}

// ============ INIT - Wait for DOM ============

window.addEventListener('DOMContentLoaded', () => {
    document.getElementById('returnPeriod')?.addEventListener('change', refreshAllData);
    document.getElementById('zscoreWindow')?.addEventListener('change', refreshAllData);
    document.getElementById('benchmark')?.addEventListener('change', refreshAllData);
    
    selectedSector = localStorage.getItem('selectedSector');
    refreshAllData();
});
