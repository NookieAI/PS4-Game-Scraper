/**
 * PS4 Game Browser — main.js
 * All data (games_cache.json) and images load from Cloudflare R2.
 * No scraping — read-only viewer of data built by scraper.py.
 */

'use strict';

const { app, BrowserWindow, ipcMain, shell, Menu } = require('electron');
const path  = require('path');
const https = require('https');
const http  = require('http');
const fs    = require('fs');

// ── CDN / config ──────────────────────────────────────────────────────────────

const CDN_BASE = 'https://pub-b111272dc86e487a95f8f7ec57b2ebb3.r2.dev';
const DATA_URL = `${CDN_BASE}/games_cache.json`;

// Allow config.json next to the exe to override defaults
const cfg = (() => {
  const locations = [
    path.join(path.dirname(process.execPath), 'config.json'),
    path.join(__dirname, 'config.json'),
  ];
  for (const loc of locations) {
    try {
      const c = JSON.parse(fs.readFileSync(loc, 'utf8'));
      console.log('[config] loaded from:', loc);
      return c;
    } catch {}
  }
  console.log('[config] using built-in defaults');
  return {};
})();

const IMAGES_BASE = (cfg.remoteImagesBase || CDN_BASE).replace(/\/$/, '');
const CACHE_URL   = cfg.remoteDataUrl || DATA_URL;

// ── In-memory state ───────────────────────────────────────────────────────────

let gamesCache  = null;   // full parsed array
let cacheByUrl  = null;   // Map<gameUrl, entry>
let lastFetched = 0;

// ── HTTP helper (no external deps — uses Node built-ins) ──────────────────────

function fetchText(url, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 5) return reject(new Error('Too many redirects'));
    const lib = url.startsWith('https://') ? https : http;
    const req = lib.get(url, { headers: { 'User-Agent': 'PS4-Game-Browser/3.0' } }, res => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        return fetchText(res.headers.location, redirects + 1).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.setTimeout(60_000, () => { req.destroy(); reject(new Error('Timeout: ' + url)); });
  });
}

// ── Image URL resolution ──────────────────────────────────────────────────────

function imgUrl(local) {
  if (!local || local === 'dead') return '';
  const normalised = local.replace(/\\/g, '/').replace(/^screenshots\//, '');
  return IMAGES_BASE + '/' + normalised;
}

// ── Data loading ──────────────────────────────────────────────────────────────

async function loadCache(force = false) {
  if (gamesCache && !force) return gamesCache;
  console.log('[data] fetching', CACHE_URL);
  const raw    = await fetchText(CACHE_URL);
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) throw new Error('games_cache.json is not a JSON array');
  gamesCache  = parsed;
  cacheByUrl  = new Map(parsed.map(e => [e.url, e]));
  lastFetched = Date.now();
  console.log(`[data] loaded ${gamesCache.length} entries`);
  return gamesCache;
}

// ── Data mappers ──────────────────────────────────────────────────────────────

function toGridEntry(e) {
  const coverShot = (e.screenshots || []).find(s => s.role === 'cover');
  return {
    id:    e.url   || '',
    url:   e.url   || '',
    title: e.title || '',
    image: imgUrl(coverShot && coverShot.local),
  };
}

function toDetailEntry(e) {
  const shots     = e.screenshots || [];
  const coverShot = shots.find(s => s.role === 'cover');

  const screenshots = shots
    .filter(s => s.role && s.role.startsWith('screenshot_'))
    .map(s => imgUrl(s.local))
    .filter(Boolean);

  const extra = e.extra || {};

  const cusaMap = new Map();
  if (extra.cusa_id) cusaMap.set(extra.cusa_id, '');
  if (Array.isArray(extra.cusa_ids)) {
    extra.cusa_ids.forEach(c => { if (c) cusaMap.set(c, ''); });
  }
  (e.releases || []).forEach(r => {
    if (r.cusa) cusaMap.set(r.cusa, r.region || cusaMap.get(r.cusa) || '');
  });

  const meta = {
    cusa_ids:  Array.from(cusaMap.entries()).map(([cusa, region]) => ({ cusa, region })),
    language:  extra.language  || '',
    voice:     extra.voice     || '',
    subtitles: extra.subtitles || '',
    note:      extra.note      || '',
  };

  const versions = (e.releases || []).map(rel => {
    const parts = [
      rel.cusa                    || '',
      rel.region ? `[${rel.region}]`  : '',
      rel.contributor ? `· ${rel.contributor}` : '',
    ].filter(Boolean);
    const header = parts.join(' ') || 'Download';

    const rows = [];
    const gameLinks = flattenLinks(rel.game || []);
    if (gameLinks.length) {
      rows.push({ label: 'Game', mirrors: gameLinks });
    }
    (rel.updates || []).forEach(upd => {
      const updLinks = flattenLinks(upd.filehosts || []);
      if (!updLinks.length) return;
      rows.push({
        label:   upd.label || upd.version || 'Update',
        mirrors: updLinks,
      });
      if (upd.notes) {
        rows.push({ label: '', mirrors: [], _note: upd.notes });
      }
    });

    return {
      header,
      rows,
      password: rel.password || '',
      note:     rel.notes    || '',
    };
  });

  return {
    title:         e.title       || '',
    cover:         imgUrl(coverShot && coverShot.local),
    screenshots,
    description:   e.description || '',
    meta,
    versions,
    _links_fetched: lastFetched,
  };
}

function flattenLinks(links) {
  const out = [];
  for (const link of (links || [])) {
    if (!link) continue;
    if (link.parts && link.parts.length) {
      link.parts.forEach((p, i) => {
        const label = p.filename
          ? path.basename(p.filename)
          : `${link.label || 'Part'} ${i + 1}`;
        out.push({ text: label, url: p.url || '' });
      });
    } else if (link.url) {
      const fn = link.filename ? ` — ${path.basename(link.filename)}` : '';
      out.push({ text: `${link.label || 'Mirror'}${fn}`, url: link.url });
    }
  }
  return out;
}

// ── Electron window ───────────────────────────────────────────────────────────

const ICON_PATH = app.isPackaged
  ? path.join(process.resourcesPath, 'assets', 'icon1.ico')
  : path.join(__dirname, 'assets', 'icon1.ico');

let mainWindow = null;

function createWindow() {
  mainWindow = new BrowserWindow({
    width:     1400,
    height:    900,
    minWidth:  800,
    minHeight: 600,
    webPreferences: {
      preload:          path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration:  false,
    },
    title:           'PS4 Game Browser',
    backgroundColor: '#000000',
    icon:            ICON_PATH,
    show:            false,
  });

  mainWindow.loadFile(path.join(__dirname, 'index.html'));

  mainWindow.once('ready-to-show', () => {
    mainWindow.maximize();
    mainWindow.show();
  });

  // Remove all menu/toolbars in production
  if (app.isPackaged) {
    Menu.setApplicationMenu(null);
    mainWindow.removeMenu && mainWindow.removeMenu();
  }

  // Block all devtools key shortcuts and context menu in production
  mainWindow.webContents.on('before-input-event', (_e, input) => {
    if (app.isPackaged) {
      const ctrl = !!input.control;
      const shift = !!input.shift;
      const alt = !!input.alt;
      const key = input.key ? input.key.toUpperCase() : '';
      if (
        key === 'F11' ||
        key === 'F12' ||
        (ctrl && shift && (key === 'I' || key === 'J' || key === 'C' || key === 'U')) ||
        (ctrl && alt && key === 'I')
      ) {
        return _e.preventDefault();
      }
    } else {
      if (input.type === 'keyDown' && input.key === 'F11') {
        mainWindow.setFullScreen(!mainWindow.isFullScreen());
      }
      if (input.type === 'keyDown' && input.key === 'F12') {
        return _e.preventDefault();
      }
    }
  });

  // Prevent devtools
  mainWindow.webContents.on('devtools-opened', () => {
    if (app.isPackaged) mainWindow.webContents.closeDevTools();
  });

  // Block context menu in production
  if (app.isPackaged) {
    mainWindow.webContents.on('context-menu', e => e.preventDefault());
  }

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: 'deny' };
  });

  if (!app.isPackaged && process.argv.includes('--dev')) {
    mainWindow.webContents.openDevTools({ mode: 'detach' });
  }
}

app.whenReady().then(() => {
  createWindow();
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

// ── IPC handlers ──────────────────────────────────────────────────────────────

ipcMain.handle('load-games-cache', async () => {
  try {
    const data = await loadCache();
    return data.map(toGridEntry);
  } catch (err) {
    console.error('[load-games-cache]', err.message);
    return [];
  }
});

ipcMain.handle('scrape-game-details', async (_e, gameUrl) => {
  try {
    await loadCache();
    const entry = cacheByUrl.get(gameUrl);
    if (!entry) return { error: `Not found: ${gameUrl}` };
    return toDetailEntry(entry);
  } catch (err) {
    console.error('[scrape-game-details]', err.message);
    return { error: err.message };
  }
});

ipcMain.handle('refresh-game-details', async (_e, gameUrl) => {
  try {
    await loadCache(true);
    const entry = cacheByUrl.get(gameUrl);
    if (!entry) return { error: `Not found: ${gameUrl}` };
    return toDetailEntry(entry);
  } catch (err) {
    console.error('[refresh-game-details]', err.message);
    return { error: err.message };
  }
});

ipcMain.handle('check-new-games', async () => {
  try {
    await loadCache(true);
    return { ok: true, count: gamesCache ? gamesCache.length : 0 };
  } catch (err) {
    console.error('[check-new-games]', err.message);
    return { ok: false, error: err.message };
  }
});

ipcMain.handle('get-data-source', () => 'remote');
ipcMain.handle('get-proxy-port', () => null);

ipcMain.handle('open-external', (_e, url) => {
  if (typeof url === 'string' && /^https?:\/\//.test(url)) {
    shell.openExternal(url);
  }
});