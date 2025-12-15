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

// Growth Score Configuration
const GROWTH_SCORE_CONFIG = {
    maxHoldings: 50, // Max holdings to analyze
    weights: {
        ret12m: 0.20,
        ret6m: 0.15,
        ret3m: 0.10,
        maxDrawdown: 0.10,
        coMoveScore: 0.15,
        trend30w: 0.05,
        rsTrend: 0.05,
        sentimentScore: 0.10,
        newsCount: 0.10
    },
    cycleBoostFactor: 0.15
};

let selectedSector = null;
let sectorZScores = {};
let sectorDataQuality = {}; // Track data quality per sector
let benchmarkPrices = null;
let isLoading = false;
let currentHoldingsSort = 'score'; // Default sort by Growth Score

const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const NEWS_CACHE_TTL_MS = 6 * 60 * 60 * 1000; // 6 hours for news
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

// Get the cache key for a ticker's price data (Yahoo or Stooq)
function getPriceCacheKey(ticker) {
    // Check Yahoo cache first, then Stooq
    if (_cache.has(`y:${ticker}`)) return `y:${ticker}`;
    if (_cache.has(`s:${ticker}`)) return `s:${ticker}`;
    return `y:${ticker}`; // Default to Yahoo
}

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

// ============ NEWS & SENTIMENT (FINNHUB) ============

async function fetchCompanyNews(symbol) {
    const cacheKey = `news:${symbol}`;
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < NEWS_CACHE_TTL_MS) return hit.data;

    try {
        const to = new Date();
        const from = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000); // 30 days ago
        const data = await fetchFromFinnhub('/company-news', {
            symbol,
            from: from.toISOString().split('T')[0],
            to: to.toISOString().split('T')[0]
        });

        const result = {
            newsCount: Array.isArray(data) ? data.length : 0,
            hasData: true
        };

        _cache.set(cacheKey, { ts: Date.now(), data: result });
        return result;
    } catch (e) {
        console.log(`News fetch failed for ${symbol}:`, e.message);
        return { newsCount: null, hasData: false };
    }
}

async function fetchNewsSentiment(symbol) {
    const cacheKey = `sentiment:${symbol}`;
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < NEWS_CACHE_TTL_MS) return hit.data;

    try {
        const data = await fetchFromFinnhub('/news-sentiment', { symbol });

        // Finnhub returns sentiment with companyNewsScore, sectorAverageBullishPercent, etc.
        const result = {
            sentimentScore: data?.companyNewsScore ?? data?.sentiment?.bullishPercent ?? null,
            buzz: data?.buzz?.buzz ?? null,
            hasData: data?.companyNewsScore != null || data?.sentiment != null
        };

        _cache.set(cacheKey, { ts: Date.now(), data: result });
        return result;
    } catch (e) {
        // Sentiment endpoint may be premium-only
        console.log(`Sentiment fetch failed for ${symbol}:`, e.message);
        return { sentimentScore: null, buzz: null, hasData: false };
    }
}

// ============ TECHNICAL METRICS CALCULATION ============

function calcReturn(prices, weeks) {
    if (!prices || prices.length < weeks + 1) return null;
    const recent = prices[prices.length - 1]?.close;
    const past = prices[prices.length - 1 - weeks]?.close;
    if (!recent || !past || past <= 0) return null;
    return ((recent / past) - 1) * 100;
}

function calcMaxDrawdown(prices, weeks) {
    if (!prices || prices.length < weeks) return null;
    const slice = prices.slice(-weeks);
    let peak = slice[0]?.close || 0;
    let maxDD = 0;

    for (const p of slice) {
        if (p.close > peak) peak = p.close;
        const dd = ((p.close - peak) / peak) * 100;
        if (dd < maxDD) maxDD = dd;
    }
    return maxDD; // Negative number
}

function calcSMAValue(prices, period) {
    if (!prices || prices.length < period) return null;
    const slice = prices.slice(-period);
    return slice.reduce((a, b) => a + b.close, 0) / period;
}

function calcRSTrend(stockPrices, sectorPrices, weeks = 8) {
    if (!stockPrices || !sectorPrices || stockPrices.length < weeks || sectorPrices.length < weeks) return null;

    // Calculate RS ratio for last 'weeks' weeks
    const rsValues = [];
    const stockSlice = stockPrices.slice(-weeks);

    for (let i = 0; i < stockSlice.length; i++) {
        const stockDate = stockSlice[i].date.getTime();
        // Find nearest sector price
        let nearestSector = null;
        let minDist = Infinity;
        for (const sp of sectorPrices.slice(-weeks * 2)) {
            const dist = Math.abs(sp.date.getTime() - stockDate);
            if (dist < minDist) {
                minDist = dist;
                nearestSector = sp;
            }
        }
        if (nearestSector && minDist < 10 * 86400000) { // Within 10 days
            rsValues.push(stockSlice[i].close / nearestSector.close);
        }
    }

    if (rsValues.length < 4) return null;

    // Simple slope calculation (is RS improving?)
    const firstHalf = rsValues.slice(0, Math.floor(rsValues.length / 2));
    const secondHalf = rsValues.slice(Math.floor(rsValues.length / 2));
    const firstAvg = firstHalf.reduce((a, b) => a + b, 0) / firstHalf.length;
    const secondAvg = secondHalf.reduce((a, b) => a + b, 0) / secondHalf.length;

    return secondAvg > firstAvg; // true if improving
}

async function calcTechnicalMetrics(symbol, sectorPrices) {
    try {
        const { prices } = await fetchStockPrices(symbol);
        if (!prices || prices.length < 52) {
            return { hasData: false };
        }

        const ret12m = calcReturn(prices, 52);
        const ret6m = calcReturn(prices, 26);
        const ret3m = calcReturn(prices, 13);
        const maxDrawdown = calcMaxDrawdown(prices, 52);
        const sma30w = calcSMAValue(prices, 30);
        const currentPrice = prices[prices.length - 1]?.close;
        const trend30w = sma30w && currentPrice ? currentPrice > sma30w : null;
        const rsTrend = calcRSTrend(prices, sectorPrices, 8);

        return {
            ret12m,
            ret6m,
            ret3m,
            maxDrawdown,
            trend30w,
            rsTrend,
            prices, // Keep for coMove calculation
            hasData: true
        };
    } catch (e) {
        console.log(`Technical metrics failed for ${symbol}:`, e.message);
        return { hasData: false };
    }
}

// ============ SECTOR TURN DETECTION ============

function detectSectorTurn(sectorTicker, benchTicker) {
    const zData = sectorZScores[sectorTicker];
    if (!zData || zData.length < 6) return false;

    const currentZ = zData[zData.length - 1]?.value;
    if (currentZ === undefined) return false;

    // Check if Z-score was below -2 in last 6 months (approx 6 data points for monthly)
    const last6Months = zData.slice(-6);
    const wasBelow2 = last6Months.some(d => d.value < -2);
    const nowAboveMinus1 = currentZ > -1;

    if (!wasBelow2 || !nowAboveMinus1) return false;

    // Check RS vs 30-week MA
    const sCache = _cache.get(getPriceCacheKey(sectorTicker));
    const bCache = _cache.get(getPriceCacheKey(benchTicker));

    if (!sCache?.data || !bCache?.data) return false;

    const sectorPrices = sCache.data;
    const benchPrices = bCache.data;

    // Calculate relative strength
    const relStrength = [];
    let bIdx = 0;
    for (let i = 0; i < sectorPrices.length; i++) {
        const sDate = sectorPrices[i].date.getTime();
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

    if (relStrength.length < 30) return false;

    const ma30 = calcSMA(relStrength, 30);
    const currentRS = relStrength[relStrength.length - 1].close;

    return ma30 && currentRS > ma30;
}

// ============ PERCENTILE & SCORING ============

function calcPercentileRank(value, allValues) {
    if (value == null || !allValues.length) return null;
    const validValues = allValues.filter(v => v != null);
    if (!validValues.length) return null;
    const below = validValues.filter(v => v < value).length;
    return (below / validValues.length) * 100;
}

function calcGrowthScore(metrics, allHoldingsMetrics, sectorTurn) {
    const weights = GROWTH_SCORE_CONFIG.weights;
    let totalWeight = 0;
    let weightedSum = 0;

    // Collect all values for percentile calculation
    const allRet12m = allHoldingsMetrics.map(m => m.ret12m).filter(v => v != null);
    const allRet6m = allHoldingsMetrics.map(m => m.ret6m).filter(v => v != null);
    const allRet3m = allHoldingsMetrics.map(m => m.ret3m).filter(v => v != null);
    const allMaxDD = allHoldingsMetrics.map(m => m.maxDrawdown).filter(v => v != null);
    const allCoMove = allHoldingsMetrics.map(m => m.coMoveScore).filter(v => v != null);
    const allSentiment = allHoldingsMetrics.map(m => m.sentimentScore).filter(v => v != null);
    const allNews = allHoldingsMetrics.map(m => m.newsCount).filter(v => v != null);

    const percentiles = {};

    // Technical returns (higher is better)
    if (metrics.ret12m != null) {
        percentiles.ret12m = calcPercentileRank(metrics.ret12m, allRet12m);
        weightedSum += percentiles.ret12m * weights.ret12m;
        totalWeight += weights.ret12m;
    }
    if (metrics.ret6m != null) {
        percentiles.ret6m = calcPercentileRank(metrics.ret6m, allRet6m);
        weightedSum += percentiles.ret6m * weights.ret6m;
        totalWeight += weights.ret6m;
    }
    if (metrics.ret3m != null) {
        percentiles.ret3m = calcPercentileRank(metrics.ret3m, allRet3m);
        weightedSum += percentiles.ret3m * weights.ret3m;
        totalWeight += weights.ret3m;
    }

    // Max drawdown (less negative is better, so invert percentile)
    if (metrics.maxDrawdown != null) {
        percentiles.maxDrawdown = 100 - calcPercentileRank(Math.abs(metrics.maxDrawdown), allMaxDD.map(Math.abs));
        weightedSum += percentiles.maxDrawdown * weights.maxDrawdown;
        totalWeight += weights.maxDrawdown;
    }

    // CoMove score (higher is better)
    if (metrics.coMoveScore != null) {
        percentiles.coMoveScore = calcPercentileRank(metrics.coMoveScore, allCoMove);
        weightedSum += percentiles.coMoveScore * weights.coMoveScore;
        totalWeight += weights.coMoveScore;
    }

    // Trend (binary)
    if (metrics.trend30w != null) {
        const trendScore = metrics.trend30w ? 100 : 0;
        weightedSum += trendScore * weights.trend30w;
        totalWeight += weights.trend30w;
    }

    // RS Trend (binary)
    if (metrics.rsTrend != null) {
        const rsScore = metrics.rsTrend ? 100 : 0;
        weightedSum += rsScore * weights.rsTrend;
        totalWeight += weights.rsTrend;
    }

    // Sentiment (higher is better)
    if (metrics.sentimentScore != null) {
        percentiles.sentimentScore = calcPercentileRank(metrics.sentimentScore, allSentiment);
        weightedSum += percentiles.sentimentScore * weights.sentimentScore;
        totalWeight += weights.sentimentScore;
    }

    // News count (higher attention is better)
    if (metrics.newsCount != null) {
        percentiles.newsCount = calcPercentileRank(metrics.newsCount, allNews);
        weightedSum += percentiles.newsCount * weights.newsCount;
        totalWeight += weights.newsCount;
    }

    // Calculate base score (re-normalized to 100)
    let score = totalWeight > 0 ? (weightedSum / totalWeight) : 0;

    // Apply cycle-end boost
    if (sectorTurn && percentiles.coMoveScore != null) {
        const boostMultiplier = 1 + (GROWTH_SCORE_CONFIG.cycleBoostFactor * (percentiles.coMoveScore / 100));
        score = Math.min(100, score * boostMultiplier);
    }

    return {
        score: Math.round(score * 10) / 10,
        percentiles,
        sectorTurn
    };
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

    const bench = document.getElementById('benchmark')?.value || 'SPY';
    const sectorTurn = detectSectorTurn(sectorTicker, bench);

    // Get sector price data for calculations
    let sectorPrices = null;
    let sectorIndex = null;
    let sectorStats = null;

    try {
        const sectorPriceData = await fetchPrices(sectorTicker);
        sectorPrices = sectorPriceData.prices;
        const sectorReturns = calcWeeklyReturns(sectorPrices.slice(-110));
        sectorStats = calcReturnStats(sectorReturns.map(r => r.value));
        if (sectorStats.count >= 10 && sectorStats.variance > 1e-6) {
            sectorIndex = buildReturnIndex(sectorReturns);
        }
    } catch (e) {
        console.log('Sector price fetch failed:', e.message);
    }

    const maxHoldings = GROWTH_SCORE_CONFIG.maxHoldings;
    const topHoldings = holdings.slice(0, maxHoldings);

    // Phase 1: Fetch quotes (fast, 5 concurrent)
    const quoteResults = await mapLimit(topHoldings, 5, async (h) => {
        try {
            const quote = await fetchFinnhubQuote(h.ticker);
            return { ...h, ...quote };
        } catch (e) {
            return { ...h, price: 0, change: 0 };
        }
    });

    let results = quoteResults
        .filter(r => r.status === 'fulfilled' && r.value)
        .map(r => r.value);

    // Phase 2: Fetch technical metrics (slower, 3 concurrent)
    const technicalResults = await mapLimit(results.slice(0, 25), 3, async (h) => {
        const tech = await calcTechnicalMetrics(h.ticker, sectorPrices);
        let coMoveScore = null;
        if (tech.prices && sectorIndex && sectorStats) {
            const stockReturns = calcWeeklyReturns(tech.prices.slice(-110));
            const pairs = [];
            for (const r of stockReturns) {
                const sv = nearestValue(sectorIndex, r.date.getTime(), 7);
                if (sv != null) pairs.push({ stock: r.value, sector: sv });
            }
            if (pairs.length >= 8) {
                const stockVals = pairs.map(p => p.stock);
                const sectorVals = pairs.map(p => p.sector);
                const stockStats = calcReturnStats(stockVals);
                const sectorMean = sectorVals.reduce((a, b) => a + b, 0) / sectorVals.length;
                if (stockStats.std > 1e-6) {
                    let cov = 0;
                    for (let i = 0; i < pairs.length; i++) {
                        cov += (stockVals[i] - stockStats.mean) * (sectorVals[i] - sectorMean);
                    }
                    cov /= Math.max(1, pairs.length - 1);
                    const corr = cov / (stockStats.std * sectorStats.std);
                    const beta = cov / sectorStats.variance;
                    if (isFinite(corr) && isFinite(beta) && corr > 0 && beta > 0) {
                        coMoveScore = corr * beta;
                    }
                }
            }
        }
        return { ...h, ...tech, coMoveScore };
    });

    results = technicalResults
        .filter(r => r.status === 'fulfilled' && r.value)
        .map(r => r.value);

    // Phase 3: Fetch news/sentiment (3 concurrent)
    const newsResults = await mapLimit(results, 3, async (h) => {
        const [news, sentiment] = await Promise.all([
            fetchCompanyNews(h.symbol || h.ticker),
            fetchNewsSentiment(h.symbol || h.ticker)
        ]);
        return {
            ...h,
            newsCount: news.newsCount,
            newsHasData: news.hasData,
            sentimentScore: sentiment.sentimentScore,
            sentimentHasData: sentiment.hasData
        };
    });

    results = newsResults
        .filter(r => r.status === 'fulfilled' && r.value)
        .map(r => r.value);

    // Phase 4: Calculate Growth Scores
    const allMetrics = results.map(r => ({
        ret12m: r.ret12m,
        ret6m: r.ret6m,
        ret3m: r.ret3m,
        maxDrawdown: r.maxDrawdown,
        coMoveScore: r.coMoveScore,
        sentimentScore: r.sentimentScore,
        newsCount: r.newsCount
    }));

    results = results.map(r => {
        const scoreData = calcGrowthScore({
            ret12m: r.ret12m,
            ret6m: r.ret6m,
            ret3m: r.ret3m,
            maxDrawdown: r.maxDrawdown,
            trend30w: r.trend30w,
            rsTrend: r.rsTrend,
            coMoveScore: r.coMoveScore,
            sentimentScore: r.sentimentScore,
            newsCount: r.newsCount
        }, allMetrics, sectorTurn);

        return {
            symbol: r.symbol || r.ticker,
            name: (r.name || r.symbol || r.ticker || '').substring(0, 24),
            price: r.price || 0,
            change: r.change || 0,
            weight: r.weight || 0,
            growthScore: scoreData.score,
            ret12m: r.ret12m,
            ret6m: r.ret6m,
            ret3m: r.ret3m,
            maxDrawdown: r.maxDrawdown,
            trend30w: r.trend30w,
            rsTrend: r.rsTrend,
            coMoveScore: r.coMoveScore,
            newsCount: r.newsCount,
            newsHasData: r.newsHasData,
            sentimentScore: r.sentimentScore,
            sentimentHasData: r.sentimentHasData,
            sectorTurn,
            percentiles: scoreData.percentiles
        };
    });

    // Default sort by Growth Score
    results.sort((a, b) => (b.growthScore ?? 0) - (a.growthScore ?? 0));

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

function sortHoldings(holdings, sortBy) {
    const sorted = [...holdings];
    switch (sortBy) {
        case 'score':
            sorted.sort((a, b) => (b.growthScore ?? 0) - (a.growthScore ?? 0));
            break;
        case 'weight':
            sorted.sort((a, b) => (b.weight ?? 0) - (a.weight ?? 0));
            break;
        case 'comove':
            sorted.sort((a, b) => (b.coMoveScore ?? -Infinity) - (a.coMoveScore ?? -Infinity));
            break;
        case 'ret12m':
            sorted.sort((a, b) => (b.ret12m ?? -Infinity) - (a.ret12m ?? -Infinity));
            break;
        default:
            sorted.sort((a, b) => (b.growthScore ?? 0) - (a.growthScore ?? 0));
    }
    return sorted;
}

function changeHoldingsSort(sortBy) {
    currentHoldingsSort = sortBy;
    if (selectedSector) {
        loadHoldings(selectedSector);
    }
}

async function loadHoldings(sectorTicker) {
    const container = document.getElementById('holdingsTable');
    if (!container) return;

    try {
        let holdings = await fetchHoldingsData(sectorTicker);

        if (!holdings.length) {
            container.innerHTML = '<div class="holdings-empty">No holdings data available</div>';
            return;
        }

        // Sort holdings based on current selection
        holdings = sortHoldings(holdings, currentHoldingsSort);

        // Check for sector turn
        const sectorTurn = holdings[0]?.sectorTurn || false;

        container.innerHTML = `
            <div class="holdings-header">
                <div class="holdings-controls">
                    <label>Sort by:</label>
                    <select id="holdingsSort" onchange="changeHoldingsSort(this.value)">
                        <option value="score" ${currentHoldingsSort === 'score' ? 'selected' : ''}>Score</option>
                        <option value="weight" ${currentHoldingsSort === 'weight' ? 'selected' : ''}>Weight</option>
                        <option value="comove" ${currentHoldingsSort === 'comove' ? 'selected' : ''}>CoMove</option>
                        <option value="ret12m" ${currentHoldingsSort === 'ret12m' ? 'selected' : ''}>12M%</option>
                    </select>
                    ${sectorTurn ? '<span class="sector-turn-badge">🔄 Cycle Turn Active</span>' : ''}
                </div>
            </div>
            <div class="holdings-table-wrap">
                <table class="holdings growth-table">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Symbol</th>
                            <th title="Growth Potential Score (0-100)">Score</th>
                            <th title="12-Month Return">12M%</th>
                            <th title="Max Drawdown (12M)">DD</th>
                            <th title="News articles (30d)">News</th>
                            <th title="Sentiment Score">Sent</th>
                            <th title="Correlation * Beta with sector">CoMove</th>
                            <th title="ETF Weight">Wt%</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${holdings.slice(0, 15).map((h, i) => `
                            <tr class="${h.sectorTurn && h.coMoveScore > 0.5 ? 'cycle-boost' : ''}">
                                <td class="rank">${i + 1}</td>
                                <td class="symbol" title="${h.name}">${h.symbol}</td>
                                <td class="score">
                                    <span class="score-value ${h.growthScore >= 70 ? 'high' : h.growthScore >= 40 ? 'medium' : 'low'}">
                                        ${h.growthScore != null ? h.growthScore.toFixed(1) : '-'}
                                    </span>
                                </td>
                                <td class="ret ${(h.ret12m ?? 0) >= 0 ? 'positive' : 'negative'}">
                                    ${h.ret12m != null ? `${h.ret12m >= 0 ? '+' : ''}${h.ret12m.toFixed(1)}%` : '<span class="data-gap">Gap</span>'}
                                </td>
                                <td class="dd ${(h.maxDrawdown ?? 0) > -15 ? 'good' : 'bad'}">
                                    ${h.maxDrawdown != null ? `${h.maxDrawdown.toFixed(1)}%` : '<span class="data-gap">Gap</span>'}
                                </td>
                                <td class="news">
                                    ${h.newsHasData ? (h.newsCount ?? 0) : '<span class="data-gap">Gap</span>'}
                                </td>
                                <td class="sentiment">
                                    ${h.sentimentHasData && h.sentimentScore != null
                                        ? `<span class="${h.sentimentScore > 0.5 ? 'positive' : h.sentimentScore < 0.3 ? 'negative' : ''}">${(h.sentimentScore * 100).toFixed(0)}</span>`
                                        : '<span class="data-gap">Gap</span>'}
                                </td>
                                <td class="comove">
                                    ${h.coMoveScore != null ? h.coMoveScore.toFixed(2) : '<span class="data-gap">Gap</span>'}
                                </td>
                                <td class="weight">${h.weight.toFixed(2)}%</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
            <div class="score-legend">
                <div class="legend-title">Score Breakdown:</div>
                <div class="legend-items">
                    <span>Technical (55%): 12M/6M/3M returns + Drawdown</span>
                    <span>Trend (25%): CoMove + Price>30wMA + RS improving</span>
                    <span>Sentiment (20%): News count + Sentiment score</span>
                </div>
            </div>
        `;
    } catch (e) {
        console.error('Holdings load error:', e);
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
