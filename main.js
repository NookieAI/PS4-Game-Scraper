const { app, BrowserWindow, ipcMain, shell, Menu } = require('electron');
const path = require('path');
const axios = require('axios');
const cheerio = require('cheerio');
const Store = require('electron-store');

const store = new Store({
  name: 'ps4-scraper-data',
  defaults: {
    gamesData: {},
    favorites: [],
    settings: {
      maxGames: 0,
      autoScan: false,
      theme: 'dark',
      defaultSort: 'date',
      cacheDays: 0,
      hostOrder: ['akira', 'viking', 'onefichier', 'letsupload', 'mediafire', 'gofile', 'rootz', 'viki']
    }
  }
});

let mainWindow;

const COMMON_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
};

function createWindow() {
  Menu.setApplicationMenu(null);
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js'),
      devTools: true
    },
    icon: path.join(__dirname, 'assets', 'icon1.ico'),
    show: false,
    autoHideMenuBar: true,
  });
  mainWindow.loadFile('index.html');
  mainWindow.once('ready-to-show', function () {
    mainWindow.maximize();
    mainWindow.show();
  });
  mainWindow.on('closed', function () { mainWindow = null; });
}

ipcMain.handle('store:get', function (e, key) {
  try { return store.get(key); } catch (err) { return null; }
});

ipcMain.handle('store:set', function (e, key, value) {
  try { store.set(key, value); return true; } catch (err) { return false; }
});

ipcMain.handle('store:delete', function (e, key) {
  try { store.delete(key); return true; } catch (err) { return false; }
});

ipcMain.handle('store:clear', function () {
  try { store.clear(); return true; } catch (err) { return false; }
});

ipcMain.handle('store:getSettings', function () {
  try { return store.get('settings'); } catch (err) {
    return { maxGames: 0, autoScan: false, theme: 'dark', defaultSort: 'date', cacheDays: 0, hostOrder: ['akira', 'viking', 'onefichier', 'letsupload', 'mediafire', 'gofile', 'rootz', 'viki'] };
  }
});

ipcMain.handle('store:setSetting', function (e, key, value) {
  try { store.set('settings.' + key, value); return true; } catch (err) { return false; }
});

ipcMain.handle('store:setSettings', function (e, settings) {
  try { store.set('settings', settings); return true; } catch (err) { return false; }
});

ipcMain.handle('open:external', function (event, url) {
  if (typeof url === 'string' && (url.indexOf('http://') === 0 || url.indexOf('https://') === 0)) {
    shell.openExternal(url);
    return true;
  }
  return false;
});

function extractVersionFromUrl(href) {
  if (!href) return '';
  var m = href.match(/[Vv_](\d{1,2}[._]\d{2,3})(?!\s*[GMKgmk][Bb])/);
  if (m) return m[1].replace('_', '.');
  m = href.match(/(\d{1,2}\.\d{3})(?!\s*[GMKgmk][Bb])/);
  if (m) return m[1];
  return '';
}

function extractVersionFromText(text) {
  if (!text) return '';
  var m = text.match(/[Vv][\s.:]*(\d{1,2}[._]\d{2,3})(?!\s*[GMKgmk][Bb])/);
  if (m) return m[1].replace('_', '.');
  return '';
}

function extractFirmwareFromContext(text) {
  if (!text) return '';
  var m = text.match(/(?:fw|firmware)\s*[:.]?\s*(\d+(?:\.[xX\d]+)?)/i);
  if (m) return m[1];
  return '';
}

function extractFirmwareFromUrl(href) {
  if (!href) return '';
  var m = href.match(/[_\/-](\d)[._](?:xx|\d{2})/i);
  if (m) return m[1];
  m = href.match(/(?:bp|fw)[\s_-]?(\d)/i);
  if (m) return m[1];
  return '';
}

function looksLikeFirmware(ver) {
  if (!ver) return false;
  if (ver.match(/^\d\.[xX]+$/i)) return true;
  if (ver.match(/^\d$/)) return true;
  var num = parseFloat(ver);
  if (!isNaN(num) && num < 10 && !ver.match(/^0\d/)) return true;
  return false;
}

function identifyHost(href) {
  if (href.indexOf('akirabox.com') !== -1 || href.indexOf('akirabox') !== -1) return 'akira';
  if (href.indexOf('vikingfile.com') !== -1 || href.indexOf('vikingfile') !== -1) return 'viking';
  if (href.indexOf('1fichier.com') !== -1) return 'onefichier';
  if (href.indexOf('letsupload') !== -1) return 'letsupload';
  if (href.indexOf('mediafire') !== -1) return 'mediafire';
  if (href.indexOf('gofile') !== -1) return 'gofile';
  if (href.indexOf('rootz') !== -1) return 'rootz';
  if (href.indexOf('viki') !== -1 && href.indexOf('viking') === -1) return 'viki';
  return '';
}

// Detect type from text. Priority: mod > game+fix > game > update > dlc > fix
function getTypeFromText(text) {
  if (!text) return '';
  var lower = text.toLowerCase();
  var hasMod = !!lower.match(/\bmod\s*menu\b/) || !!lower.match(/\bmod\b/);
  var hasGame = !!lower.match(/\bgame\b/);
  var hasFix = !!lower.match(/\bfix\b/);
  var hasUpdate = !!lower.match(/\bupdate\b/);
  var hasDlc = !!lower.match(/\bdlc\b/);

  // "Mod Menu" or "Update + Mod Menu" = mod
  if (hasMod) return 'mod';
  // "Game (Fix)" or "Game + Fix" = game
  if (hasGame && hasFix) return 'game';
  if (hasGame) return 'game';
  if (hasUpdate) return 'update';
  if (hasDlc) return 'dlc';
  if (hasFix) return 'fix';
  return '';
}

// Section header type detection
function getSectionTypeFromHeader(text) {
  if (!text) return '';
  var lower = text.toLowerCase().trim();
  if (lower.match(/mod\s*menu/i)) return 'mod';
  if (lower.match(/^game\b/)) return 'game';
  if (lower.match(/^update\b/)) return 'update';
  if (lower.match(/^fix\b/)) return 'fix';
  if (lower.match(/^dlc\b/)) return 'dlc';
  if (lower.match(/^mod\b/)) return 'mod';
  return '';
}

// Extract the meaningful label from a pack line
function extractPackLabel(text) {
  if (!text) return '';
  var colonIdx = text.indexOf(':');
  var label = colonIdx !== -1 ? text.substring(0, colonIdx).trim() : text.trim();
  var dashIdx = label.indexOf(' \u2013 ');
  if (dashIdx === -1) dashIdx = label.indexOf(' - ');
  if (dashIdx !== -1) label = label.substring(0, dashIdx).trim();
  label = label
    .replace(/\b(lets|mediafire|buznew|akia|viki|rootz|1file|akira|viking|1fichier|gofile|letsupload|mega)\b/gi, '')
    .replace(/\s{2,}/g, ' ')
    .replace(/^[\s,;.\-\u2013\u2014:+]+|[\s,;.\-\u2013\u2014:+]+$/g, '')
    .trim();
  return label;
}

function isValidScreenshot(src) {
  if (!src) return false;
  if (!src.match(/\.(jpg|jpeg|png|webp|gif)(\?|#|$)/i)) return false;
  if (src.indexOf('data:') === 0) return false;
  if (src.indexOf('.svg') !== -1 || src.indexOf('emoji') !== -1 || src.indexOf('smilies') !== -1) return false;
  if (src.indexOf('/icon/') !== -1 || src.indexOf('/icons/') !== -1) return false;
  if (src.indexOf('/logo/') !== -1 || src.indexOf('/logos/') !== -1) return false;
  if (src.indexOf('avatar') !== -1 || src.indexOf('gravatar') !== -1) return false;
  if (src.indexOf('/ads/') !== -1 || src.indexOf('/ad/') !== -1) return false;
  if (src.indexOf('/banner/') !== -1 || src.indexOf('/banners/') !== -1) return false;
  if (src.indexOf('widget') !== -1 || src.indexOf('pixel') !== -1 || src.indexOf('tracking') !== -1 || src.indexOf('1x1') !== -1) return false;
  return true;
}

function normalizeImgUrl(url) {
  if (!url) return '';
  return url.replace(/-\d{2,4}x\d{2,4}(?=\.\w{3,4}(?:\?|#|$))/, '');
}

ipcMain.handle('fetch:gameList', async function (event, pageUrl, timeout) {
  try {
    var response = await axios.get(pageUrl, { timeout: timeout || 10000, headers: COMMON_HEADERS });
    var $ = cheerio.load(response.data);
    var games = [];
    var strategies = [
      { container: '.post', title: 'h2 a', date: '.publish-date, time, .entry-date, .post-date', cover: 'img' },
      { container: 'article', title: 'h2 a, h3 a, .entry-title a', date: 'time, .entry-date, .published, .post-date', cover: 'img, .post-thumbnail img' },
      { container: '.hentry, .entry, .type-post', title: 'h2 a, h3 a, .entry-title a', date: 'time, .entry-date, .published', cover: 'img' },
    ];
    for (var s = 0; s < strategies.length; s++) {
      var strat = strategies[s];
      var elements = $(strat.container);
      if (elements.length === 0) continue;
      elements.each(function (i, el) {
        var $p = $(el);
        var $l = $p.find(strat.title).first();
        var title = $l.text().trim() || $l.attr('title') || '';
        var url = $l.attr('href') || '';
        var $d = $p.find(strat.date).first();
        var date = $d.attr('datetime') || $d.text().trim() || '';
        var coverEl = $p.find(strat.cover).first();
        var cover = coverEl.attr('src') || coverEl.attr('data-src') || coverEl.attr('data-lazy-src') || coverEl.attr('data-original') || '';
        if (title && url) games.push({ title: title.replace(/\s+/g, ' ').trim(), url: url, date: date, cover: cover });
      });
      if (games.length > 0) break;
    }
    return { success: true, games: games, error: null, endOfList: false };
  } catch (error) {
    var status = error.response ? error.response.status : null;
    if (status === 404) return { success: true, games: [], error: null, endOfList: true };
    return { success: false, games: [], endOfList: false, error: { url: pageUrl, message: error.message, code: error.code || null, status: status } };
  }
});

ipcMain.handle('fetch:rss', async function (event, rssUrl) {
  try {
    var response = await axios.get(rssUrl, { timeout: 30000, headers: COMMON_HEADERS });
    var $ = cheerio.load(response.data, { xmlMode: true });
    var rssData = {};
    $('item').each(function (i, el) {
      var title = $(el).find('title').text().trim();
      var link = $(el).find('link').text().trim();
      var pubDate = $(el).find('pubDate').text().trim();
      var cover = $(el).find('enclosure').attr('url') || '';
      var date = '';
      if (pubDate) {
        try { date = new Date(pubDate).toISOString().split('T')[0]; } catch (e) { date = ''; }
      }
      if (title && link) rssData[title] = { cover: cover, date: date, url: link };
    });
    return { success: true, data: rssData, error: null };
  } catch (error) {
    return { success: false, data: {}, error: { url: rssUrl, message: error.message, code: error.code || null, status: error.response ? error.response.status : null } };
  }
});

var scrapeLocksInProgress = new Map();

ipcMain.handle('fetch:gamePage', async function (event, gameUrl, gameTitle) {
  var key = gameTitle || gameUrl;
  var lockTimeout = 30000;
  var waitStart = Date.now();
  while (scrapeLocksInProgress.has(key)) {
    if (Date.now() - waitStart > lockTimeout) { scrapeLocksInProgress.delete(key); break; }
    await scrapeLocksInProgress.get(key);
  }
  var releaseLock;
  var lockPromise = new Promise(function (resolve) { releaseLock = resolve; });
  scrapeLocksInProgress.set(key, lockPromise);

  try {
    var response = await axios.get(gameUrl, { timeout: 30000, headers: COMMON_HEADERS });
    var $ = cheerio.load(response.data);
    var seenUrls = {};
    var entryContent = $('.entry-content');
    var pageText = entryContent.text() || '';

    var pageCusaMatch = pageText.match(/(CUSA\d{4,6})/i);
    var pageCusa = pageCusaMatch ? pageCusaMatch[1].toUpperCase() : '';

    var globalVersion = '';
    var gvm = pageText.match(/[Vv][\s.:]*(\d{1,2}[._]\d{2,3})(?!\s*[GMKgmk][Bb])/);
    if (gvm) globalVersion = gvm[1].replace('_', '.');

    var globalFirmware = '';
    var gfm = pageText.match(/(?:Working|Works)\s*(?:on)?\s*[:.]?\s*([\d]+(?:\.[\dxX]+)?)/i);
    if (gfm) {
      globalFirmware = gfm[1].trim();
      var gfn = globalFirmware.match(/(\d+)(?=\.[\dxX])/);
      if (gfn) globalFirmware = gfn[1] + '.xx';
    }

    // ===== PASS 1: Build ordered content blocks =====
    var linkTypeMap = {};
    var linkFwMap = {};
    var linkSectionVer = {};
    var linkDescMap = {};

    var secType = '';
    var secFw = '';
    var secVer = '';

    var contentBlocks = [];
    entryContent.children().each(function (i, el) {
      var txt = $(el).text().trim();
      var linksInEl = $(el).find('a[href]');
      var linkCount = linksInEl.length;

      if (linkCount === 0 && txt.length > 1) {
        contentBlocks.push({ type: 'text', text: txt });
      } else if (linkCount > 0) {
        var html = $(el).html() || '';
        var linkEls = [];
        linksInEl.each(function (j, aEl) {
          var h = $(aEl).attr('href');
          if (h && identifyHost(h)) linkEls.push({ href: h, html: $.html(aEl) });
        });
        if (linkEls.length === 0) return;

        var lastEnd = 0;
        for (var li = 0; li < linkEls.length; li++) {
          var linkHtml = linkEls[li].html;
          var linkIdx = html.indexOf(linkHtml, lastEnd);
          if (linkIdx < 0) continue;
          var textBefore = html.substring(lastEnd, linkIdx).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
          if (textBefore) contentBlocks.push({ type: 'text', text: textBefore });
          contentBlocks.push({ type: 'link', href: linkEls[li].href });
          lastEnd = linkIdx + linkHtml.length;
        }
        var textAfter = html.substring(lastEnd).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
        if (textAfter && textAfter.length > 2) contentBlocks.push({ type: 'text', text: textAfter });
      }
    });

    // ===== Process blocks in order =====
    var currentPackLabel = '';
    var currentPackType = '';
    var currentPackVer = '';

    for (var bi = 0; bi < contentBlocks.length; bi++) {
      var block = contentBlocks[bi];

      if (block.type === 'text') {
        var txt = block.text;
        var lower = txt.toLowerCase();

        // Section header? (e.g. "Game:", "Update:", "Mod Menu:")
        var headerType = getSectionTypeFromHeader(txt);
        if (headerType) {
          secType = headerType;
          secFw = extractFirmwareFromContext(txt) || '';
          var hv = extractVersionFromText(txt);
          if (hv) secVer = hv;
          currentPackLabel = '';
          currentPackType = '';
          currentPackVer = '';
          continue;
        }

        // Title line with CUSA? (e.g. "CUSA00411 – EUR (v1.53)(Mod Menu)")
        if (lower.match(/cusa\d{4,6}/i)) {
          var titleType = getTypeFromText(txt);
          if (titleType) {
            secType = titleType;
            var tv = extractVersionFromText(txt);
            if (tv) secVer = tv;
          }
          currentPackLabel = '';
          currentPackType = '';
          currentPackVer = '';
          continue;
        }

        // Pack description line?
        var isPackLine = lower.match(/\bgame\b/) || lower.match(/\bupdate\b/) ||
                          lower.match(/\bfix\b/) || lower.match(/\bdlc\b/) ||
                          lower.match(/\bmod\b/);
        if (isPackLine) {
          currentPackLabel = extractPackLabel(txt);
          currentPackType = getTypeFromText(txt);
          currentPackVer = extractVersionFromText(txt);
          continue;
        }
      }

      if (block.type === 'link') {
        var href = block.href;
        if (currentPackLabel) linkDescMap[href] = currentPackLabel;
        if (currentPackType && !linkTypeMap[href]) linkTypeMap[href] = currentPackType;
        else if (secType && !linkTypeMap[href]) linkTypeMap[href] = secType;

        var urlVer = extractVersionFromUrl(href);
        if (!urlVer && currentPackVer) linkSectionVer[href] = currentPackVer;
        else if (!urlVer && secVer) linkSectionVer[href] = secVer;

        if (secFw && !linkFwMap[href]) linkFwMap[href] = secFw;
      }
    }

    // ===== PASS 2: Collect download links =====
    var akiraLinks = [];
    var vikingLinks = [];
    var onefichierLinks = [];
    var otherLinks = [];

    entryContent.find('a[href]').each(function (i, el) {
      var href = $(el).attr('href');
      if (!href) return;
      if (seenUrls[href]) return;
      seenUrls[href] = true;
      var host = identifyHost(href);
      if (!host) return;

      var linkText = $(el).text().trim() || '';
      var parentP = $(el).closest('p');
      var parentDiv = $(el).closest('div');
      var contextText = parentP.length ? parentP.text().trim().replace(/\s+/g, ' ') : '';
      if (!contextText && parentDiv.length) contextText = parentDiv.text().trim().replace(/\s+/g, ' ');

      var ver = extractVersionFromUrl(href);
      if (!ver) ver = extractVersionFromText(linkText);
      if (!ver && linkSectionVer[href]) ver = linkSectionVer[href];
      if (!ver && globalVersion) ver = globalVersion;

      var fwVer = linkFwMap[href] || extractFirmwareFromContext(linkText) || extractFirmwareFromUrl(href);
      if (!fwVer && globalFirmware) fwVer = globalFirmware;

      var type = linkTypeMap[href] || '';
      if (!type) type = getTypeFromText(contextText);
      if (!type) type = 'game';

      if (type === 'fix' && ver && !fwVer && looksLikeFirmware(ver)) {
        fwVer = ver;
        ver = '';
      }

      var desc = linkDescMap[href] || '';

      var ld = {
        link: href, host: host,
        version: contextText || linkText || 'Download Link',
        extractedVersion: ver, extractedFirmware: fwVer,
        type: type, packDescription: desc
      };
      if (host === 'akira') akiraLinks.push(ld);
      else if (host === 'viking') vikingLinks.push(ld);
      else if (host === 'onefichier') onefichierLinks.push(ld);
      else otherLinks.push(ld);
    });

    var title = $('title').text() || '';
    var metaDesc = $('meta[name="description"]').attr('content') || '';
    var cover = $('meta[property="og:image"]').attr('content') || '';

    var voice = (title.match(/Voice\s*:\s*(.+?)(?=\s*(?:Subtitles?|Note|Size|$))/i) || [null, ''])[1] || '';
    voice = voice.replace(/\s*\|?\s*$/, '').trim();
    var subtitles = (title.match(/Subtitles?\s*:\s*(.+?)(?=\s*(?:Note|Size|Voice|$))/i) || [null, ''])[1] || '';
    subtitles = subtitles.replace(/\s*\|?\s*$/, '').trim();
    var notes = (title.match(/Note\s*:\s*(.+?)(?=\s*(?:Size|Voice|Subtitles?|$))/i) || [null, ''])[1] || '';
    notes = notes.replace(/\s*\|?\s*$/, '').trim();
    var size = (title.match(/Size\s*:\s*(.+?)(?=\s*(?:Voice|Note|Subtitles?|$))/i) || [null, ''])[1] || '';
    size = size.replace(/\s*\|?\s*$/, '').trim();

    var fullContent = entryContent.text();

    var firmware = '';
    var fwMatch = fullContent.match(/(?:Working|Works)\s*(?:on)?\s*[:.]?\s*([\d]+(?:\.[\dxX]+)?(?:\s*[-\u2013]\s*[\d]+(?:\.[\dxX]+)?)?)/i);
    if (fwMatch) {
      firmware = fwMatch[1].trim();
      var fwNumbers = firmware.match(/(\d+)(?=\.[\dxX])/g);
      if (fwNumbers && fwNumbers.length > 0) {
        var nums = fwNumbers.map(function (v) { return parseInt(v); });
        firmware = Math.min.apply(null, nums) + '.xx and higher';
      }
    }

    var date = $('meta[property="article:published_time"]').attr('content') || $('time').attr('datetime') || '';

    var description = '';
    var blockquoteText = entryContent.find('blockquote').text().trim();
    if (blockquoteText && blockquoteText.length > 30) {
      description = blockquoteText;
    } else {
      entryContent.find('p').each(function (i, el) {
        var pText = $(el).text().trim();
        if (pText.length < 30) return;
        if (pText.match(/akirabox|vikingfile|1fichier|mediafire|download/i)) return;
        if (pText.match(/^(Game|Update|Fix|DLC)\s*:/i)) return;
        description = pText;
        return false;
      });
      if (!description) description = metaDesc || '';
    }

    var password = (fullContent.match(/Password\s*:\s*(\S+)/i) || [null, ''])[1] || '';
    password = password.trim();

    var screenLanguages = (fullContent.match(/Screen Languages\s*:\s*(.+?)(?=\n|Password|Guide|$)/i) || [null, ''])[1] || '';
    screenLanguages = screenLanguages.trim();

    var guide = (fullContent.match(/Guide\s*:\s*(.+?)(?=\n|Password|Screen Languages|$)/i) || [null, ''])[1] || '';
    guide = guide.trim();

    var cusa = pageCusa || null;

    var screenshots = [];
    var seenScreenshots = {};
    var baseUrl = 'https://dlpsgame.com';
    var maxScreenshots = 2;
    var coverNorm = cover ? normalizeImgUrl(cover) : '';

    function tryAddScreenshot(src) {
      if (screenshots.length >= maxScreenshots) return false;
      if (!src) return true;
      var fullSrc = src.indexOf('http') === 0 ? src : baseUrl + src;
      if (!isValidScreenshot(fullSrc)) return true;
      var srcNorm = normalizeImgUrl(fullSrc);
      if (coverNorm && srcNorm === coverNorm) return true;
      if (seenScreenshots[srcNorm]) return true;
      seenScreenshots[srcNorm] = true;
      screenshots.push(fullSrc);
      return screenshots.length < maxScreenshots;
    }

    entryContent.find('img').each(function (i, el) {
      if (screenshots.length >= maxScreenshots) return false;
      var src = $(el).attr('src') || $(el).attr('data-src') || $(el).attr('data-lazy-src') || $(el).attr('data-original') || '';
      if (!src) return;
      var width = $(el).attr('width'); var height = $(el).attr('height');
      if (width && parseInt(width) < 100) return;
      if (height && parseInt(height) < 100) return;
      tryAddScreenshot(src);
    });

    if (screenshots.length < maxScreenshots) {
      entryContent.find('a[href]').each(function (i, el) {
        if (screenshots.length >= maxScreenshots) return false;
        var href = $(el).attr('href') || '';
        if (!href.match(/\.(jpg|jpeg|png|webp|gif)(\?|#|$)/i)) return;
        tryAddScreenshot(href);
      });
    }

    if (screenshots.length < maxScreenshots) {
      $('img').each(function (i, el) {
        if (screenshots.length >= maxScreenshots) return false;
        var src = $(el).attr('src') || $(el).attr('data-src') || $(el).attr('data-lazy-src') || '';
        if (!src) return;
        var width = $(el).attr('width'); var height = $(el).attr('height');
        if (width && parseInt(width) < 200) return;
        if (height && parseInt(height) < 200) return;
        tryAddScreenshot(src);
      });
    }

    if (screenshots.length === 0 && cover) screenshots.push(cover);

    return {
      success: true,
      data: {
        akira: akiraLinks, viking: vikingLinks, onefichier: onefichierLinks, other: otherLinks,
        title: title, metaDesc: metaDesc, cover: cover, voice: voice, subtitles: subtitles,
        notes: notes, size: size, firmware: firmware, date: date, description: description,
        screenshots: screenshots, password: password, screenLanguages: screenLanguages,
        guide: guide, cusa: cusa
      },
      error: null
    };
  } catch (error) {
    return {
      success: false,
      data: {
        akira: [], viking: [], onefichier: [], other: [],
        title: '', metaDesc: '', cover: '', voice: '', subtitles: '',
        notes: '', size: '', firmware: '', date: '', description: '',
        screenshots: [], password: '', screenLanguages: '', guide: '', cusa: null
      },
      error: {
        url: gameUrl, gameTitle: gameTitle || 'Unknown',
        message: error.message, code: error.code || null,
        status: error.response ? error.response.status : null
      }
    };
  } finally {
    scrapeLocksInProgress.delete(key);
    releaseLock();
  }
});

app.on('ready', createWindow);
app.on('window-all-closed', function () { if (process.platform !== 'darwin') app.quit(); });
app.on('activate', function () { if (mainWindow === null) createWindow(); });