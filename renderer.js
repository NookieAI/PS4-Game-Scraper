document.addEventListener('DOMContentLoaded', function () {
  function $(id) { return document.getElementById(id); }

  var scrapeBtn = $('scrapeBtn');
  var cancelBtn = $('cancelBtn');
  var clearCacheBtn = $('clearCacheBtn');
  var retryBtn = $('retryBtn');
  var sortBtn = $('sortBtn');
  var favBtn = $('favBtn');
  var statusDiv = $('status');
  var resultsDiv = $('results');
  var searchInput = $('searchInput');
  var notificationDiv = $('notification');
  var discordLink = $('discord-link');
  var modal = $('modal');
  var modalBody = $('modal-body');
  var closeBtn = modal ? modal.querySelector('.close') : null;
  var fullImageModal = $('fullImageModal');
  var fullImage = $('fullImage');
  var fullImageClose = fullImageModal ? fullImageModal.querySelector('.close') : null;
  var settingsBtn = $('settingsBtn');
  var settingsModal = $('settingsModal');
  var settingsClose = settingsModal ? settingsModal.querySelector('.close') : null;
  var saveSettingsBtn = $('saveSettingsBtn');
  var progressCenter = $('progressCenter');
  var progressSpinner = $('progressSpinner');
  var statGames = $('statGames');
  var statPages = $('statPages');
  var statErrors = $('statErrors');

  var BASE_URL = 'https://dlpsgame.com';
  var CATEGORY = '/category/ps4/';
  var RSS_PATH = '/category/ps4/feed/';
  var DISCORD_URL = 'https://discord.gg/wp3WpWXP77';
  var isCancelled = false;
  var gamesData = {};
  var sortByDate = true;
  var showFavoritesOnly = false;
  var favorites = [];
  var currentGamesFound = 0;
  var currentPagesScanned = 0;
  var fuseInstance = null;
  var maxGames = 0;
  var scrollbarWidth = 0;
  var isScraping = false;
  var modalLoading = false;
  var fetchErrors = { total: 0, timeouts: 0, serverErrors: 0, rateLimited: 0, other: 0, details: [] };
  var searchDebounceTimer = null;
  var lastFuseDataCount = -1;

  var HOST_KEYS = ['akira', 'viking', 'onefichier', 'other'];
  var ALL_HOSTS = ['akira', 'viking', 'onefichier', 'letsupload', 'mediafire', 'gofile', 'rootz', 'viki'];
  var HOST_LABELS = {
    akira: 'Akira', viking: 'Viking', onefichier: '1Fichier',
    letsupload: 'LetsUpload', mediafire: 'Mediafire', gofile: 'Gofile',
    rootz: 'Rootz', viki: 'Viki', other: 'Other'
  };

  var settings = {
    maxGames: 0, autoScan: false, theme: 'dark',
    defaultSort: 'date', cacheDays: 0, hostOrder: ALL_HOSTS.slice()
  };

  function getHostDisplayName(item) {
    if (item.host && HOST_LABELS[item.host]) return HOST_LABELS[item.host];
    return HOST_LABELS.other;
  }

  function getScrollbarWidth() {
    if (scrollbarWidth > 0) return scrollbarWidth;
    var outer = document.createElement('div');
    outer.style.visibility = 'hidden'; outer.style.overflow = 'scroll'; outer.style.width = '100px';
    document.body.appendChild(outer); scrollbarWidth = outer.offsetWidth - outer.clientWidth;
    document.body.removeChild(outer); return scrollbarWidth;
  }

  function lockBodyScroll() { var sw = getScrollbarWidth(); document.body.style.overflow = 'hidden'; if (sw > 0) document.body.style.paddingRight = sw + 'px'; }
  function unlockBodyScroll() { document.body.style.overflow = ''; document.body.style.paddingRight = ''; }
  function isAnyModalOpen() { return (modal && modal.style.display === 'block') || (fullImageModal && fullImageModal.style.display === 'block') || (settingsModal && settingsModal.style.display === 'block'); }
  function openModal(m) { if (m) { m.style.display = 'block'; lockBodyScroll(); } }
  function closeModalFn(m) { if (m) { m.style.display = 'none'; if (!isAnyModalOpen()) unlockBodyScroll(); } }

  function resetFetchErrors() { fetchErrors = { total: 0, timeouts: 0, serverErrors: 0, rateLimited: 0, other: 0, details: [] }; }
  function trackError(ei) {
    if (!ei) return; fetchErrors.total++;
    if (ei.code === 'ECONNABORTED' || (ei.message && ei.message.indexOf('timeout') !== -1)) fetchErrors.timeouts++;
    else if (ei.status === 429) fetchErrors.rateLimited++;
    else if (ei.status && ei.status >= 500) fetchErrors.serverErrors++;
    else fetchErrors.other++;
    if (fetchErrors.details.length < 50) fetchErrors.details.push(ei);
    if (statErrors) statErrors.textContent = fetchErrors.total;
  }
  function getErrorSummary() {
    if (fetchErrors.total === 0) return '';
    var p = [];
    if (fetchErrors.timeouts > 0) p.push(fetchErrors.timeouts + ' timeouts');
    if (fetchErrors.rateLimited > 0) p.push(fetchErrors.rateLimited + ' rate-limited');
    if (fetchErrors.serverErrors > 0) p.push(fetchErrors.serverErrors + ' server errors');
    if (fetchErrors.other > 0) p.push(fetchErrors.other + ' other');
    return fetchErrors.total + ' failed (' + p.join(', ') + ')';
  }

  if (!window.api) { if (statusDiv) statusDiv.textContent = 'ERROR: App failed to initialize'; return; }
  function safeOpenExternal(url) { if (window.api && typeof window.api.openExternal === 'function') window.api.openExternal(url); }
  function safeDesktopNotification(title, body) {
    if (window.api && typeof window.api.showDesktopNotification === 'function') {
      try { window.api.showDesktopNotification(title, body); } catch (e) {}
    }
  }

  function clearSearchInput() {
    if (searchInput) searchInput.value = '';
    if (searchDebounceTimer) { clearTimeout(searchDebounceTimer); searchDebounceTimer = null; }
  }

  function reapplySearch() {
    var q = searchInput ? searchInput.value.trim() : '';
    if (q) filterCardsInPlace(q);
  }

  function updateFavBtn() {
    if (!favBtn) return;
    var count = favorites.length;
    favBtn.style.color = showFavoritesOnly ? '#ffd700' : '#ccc';
    favBtn.innerHTML = count > 0 ? '&#9733; <span class="fav-count">' + count + '</span>' : '&#9733;';
  }
  updateFavBtn();

  function showCancel() { if (cancelBtn) { cancelBtn.style.display = 'inline-block'; cancelBtn.disabled = false; } }
  function hideCancel() { if (cancelBtn) { cancelBtn.style.display = 'none'; cancelBtn.disabled = true; } }
  function showRetry() { if (retryBtn) { retryBtn.style.display = 'inline-block'; retryBtn.disabled = false; } }
  function hideRetry() { if (retryBtn) { retryBtn.style.display = 'none'; retryBtn.disabled = true; } }

  function showProgress() {
    if (statGames) statGames.textContent = '0';
    if (statPages) statPages.textContent = '0';
    if (statErrors) statErrors.textContent = '0';
    if (progressSpinner) progressSpinner.classList.remove('done');
    if (progressCenter) progressCenter.classList.add('visible');
  }
  function hideProgress() { setTimeout(function () { if (progressCenter) progressCenter.classList.remove('visible'); }, 3000); }
  function completeProgress(total) { if (progressSpinner) progressSpinner.classList.add('done'); if (statGames) statGames.textContent = total; }
  function updateProgressStats() { if (statGames) statGames.textContent = currentGamesFound; if (statPages) statPages.textContent = currentPagesScanned; }

  function escapeHtml(t) { if (!t) return ''; var d = document.createElement('div'); d.textContent = String(t); return d.innerHTML; }
  function safeText(el, t) { if (el) el.textContent = String(t || ''); }

  function formatFirmwareVersion(ver) {
    if (!ver) return '';
    var v = ver.replace(/\.?[xX]+$/i, '');
    if (v.match(/^\d+$/)) return v + '.xx';
    return ver;
  }

  function getAllBackportFirmwares(gameData) {
    if (!gameData) return [];
    var allLinks = [];
    for (var k = 0; k < HOST_KEYS.length; k++) {
      if (gameData[HOST_KEYS[k]]) allLinks = allLinks.concat(gameData[HOST_KEYS[k]]);
    }
    var fwSet = {};
    for (var i = 0; i < allLinks.length; i++) {
      var item = allLinks[i];
      if (item.type !== 'backport') continue;
      var fw = item.extractedFirmware || item.extractedVersion || '';
      if (!fw) continue;
      var numStr = fw.replace(/[._][xX\d]+$/, '');
      var num = parseInt(numStr, 10);
      if (!isNaN(num) && num < 20) fwSet[num] = true;
    }
    return Object.keys(fwSet).map(function (n) { return parseInt(n, 10); }).sort(function (a, b) { return a - b; });
  }

  function getBackportFwGroup(item) {
    var fw = item.extractedFirmware || item.extractedVersion || '';
    if (!fw) return '0';
    var numStr = fw.replace(/[._][xX\d]+$/, '');
    var num = parseInt(numStr, 10);
    return (!isNaN(num) && num < 20) ? String(num) : '0';
  }

  function cleanDescription(t) {
    if (!t) return '';
    var junkPatterns = [/this game includes optional in-game purchases.*?virtual in-game items\.?/gi, /this game includes optional in-game purchases.*$/gim, /in-game purchases of virtual currency.*?virtual in-game items\.?/gi, /this game includes.*?in-game purchases.*?\.?/gi, /requires a persistent internet connection.*?\.?/gi, /internet connection required.*?\.?/gi, /online features require.*?subscription\.?/gi, /ps plus.*?required.*?\.?/gi, /playstation plus.*?required.*?\.?/gi, /©\s*\d{4}.*$/gim, /all rights reserved\.?/gi, /password\s*:\s*\S+/gi, /dlpsgame\.com/gi];
    var cleaned = t;
    for (var i = 0; i < junkPatterns.length; i++) cleaned = cleaned.replace(junkPatterns[i], '');
    cleaned = cleaned.replace(/[""\u201C\u201D]{2,}/g, '');
    cleaned = cleaned.replace(/\s{2,}/g, ' ').replace(/^\s+|\s+$/g, '').replace(/^[,;.\s]+|[,;.\s]+$/g, '');
    return cleaned;
  }

  function formatDescription(t) {
    if (!t || !t.trim()) return '';
    var e = escapeHtml(t);
    if (t.indexOf('\u2022') !== -1) {
      var parts = e.split('\u2022');
      var h = '<p>' + parts[0].trim() + '</p>';
      if (parts.length > 1) { h += '<ul>'; for (var i = 1; i < parts.length; i++) { var li = parts[i].trim(); if (li) h += '<li>' + li + '</li>'; } h += '</ul>'; }
      return h;
    }
    var sentences = e.replace(/([.!?])\s+/g, '$1\n').split('\n').map(function (s) { return s.trim(); }).filter(function (s) { return s.length > 0; });
    if (sentences.length <= 1) return '<p>' + e + '</p>';
    var html = '';
    var chunk = [];
    for (var m = 0; m < sentences.length; m++) {
      chunk.push(sentences[m]);
      if (chunk.length >= 3 || m === sentences.length - 1) { html += '<p>' + chunk.join(' ') + '</p>'; chunk = []; }
    }
    return html;
  }

  function showNotification(msg, type) {
    type = type || 'info'; safeText(notificationDiv, msg);
    if (notificationDiv) {
      notificationDiv.style.background = type === 'error' ? '#e74c3c' : type === 'success' ? '#27ae60' : 'rgba(52, 152, 219, 0.5)';
      notificationDiv.style.display = 'block';
      setTimeout(function () { notificationDiv.style.display = 'none'; }, 5000);
    }
  }

  function setButtonsDuringScan(s) {
    if (scrapeBtn) scrapeBtn.disabled = s;
    if (s) showCancel(); else hideCancel();
    if (clearCacheBtn) clearCacheBtn.disabled = s;
    if (sortBtn) sortBtn.disabled = s;
    if (favBtn) favBtn.disabled = s;
    if (settingsBtn) settingsBtn.disabled = s;
    if (searchInput) searchInput.disabled = s;
  }

  function applyTheme(theme) {
    if (theme === 'light') document.body.classList.add('light-theme');
    else document.body.classList.remove('light-theme');
  }

  function checkCacheExpiry() {
    if (settings.cacheDays <= 0) return;
    var lastScan = null;
    var titles = Object.keys(gamesData);
    for (var i = 0; i < titles.length; i++) {
      var d = gamesData[titles[i]].date;
      if (d && (!lastScan || d > lastScan)) lastScan = d;
    }
    if (!lastScan) return;
    var diffDays = Math.floor((new Date() - new Date(lastScan)) / (1000 * 60 * 60 * 24));
    if (diffDays >= settings.cacheDays) {
      gamesData = {};
      window.api.store.delete('gamesData');
      showNotification('Cache auto-cleared (' + diffDays + ' days old)', 'info');
    }
  }

  function populateSettingsUI() {
    var mi = $('maxGamesInput'); if (mi) mi.value = settings.maxGames;
    var as = $('autoScanToggle'); if (as) as.checked = settings.autoScan;
    var th = $('themeSelect'); if (th) th.value = settings.theme;
    var ds = $('defaultSortSelect'); if (ds) ds.value = settings.defaultSort;
    var cd = $('cacheDaysInput'); if (cd) cd.value = settings.cacheDays;
    renderHostOrder();
  }

  function renderHostOrder() {
    var container = $('hostOrderList');
    if (!container) return;
    container.innerHTML = '';
    var order = settings.hostOrder && settings.hostOrder.length > 0 ? settings.hostOrder : ALL_HOSTS.slice();
    for (var i = 0; i < order.length; i++) {
      (function (idx) {
        var hostId = order[idx];
        var row = document.createElement('div'); row.className = 'host-order-item';
        var label = document.createElement('span'); label.className = 'host-order-label';
        safeText(label, HOST_LABELS[hostId] || hostId); row.appendChild(label);
        var controls = document.createElement('span'); controls.className = 'host-order-controls';
        var upBtn = document.createElement('button'); upBtn.className = 'host-order-btn'; upBtn.textContent = '\u25B2'; upBtn.disabled = idx === 0;
        upBtn.addEventListener('click', function () { if (idx > 0) { var tmp = settings.hostOrder[idx - 1]; settings.hostOrder[idx - 1] = settings.hostOrder[idx]; settings.hostOrder[idx] = tmp; renderHostOrder(); } });
        controls.appendChild(upBtn);
        var downBtn = document.createElement('button'); downBtn.className = 'host-order-btn'; downBtn.textContent = '\u25BC'; downBtn.disabled = idx === order.length - 1;
        downBtn.addEventListener('click', function () { if (idx < settings.hostOrder.length - 1) { var tmp = settings.hostOrder[idx + 1]; settings.hostOrder[idx + 1] = settings.hostOrder[idx]; settings.hostOrder[idx] = tmp; renderHostOrder(); } });
        controls.appendChild(downBtn);
        row.appendChild(controls); container.appendChild(row);
      })(i);
    }
  }

  function collectSettings() {
    var mi = $('maxGamesInput'); var v = mi ? parseInt(mi.value, 10) : 0;
    settings.maxGames = (isNaN(v) || v < 0) ? 0 : v; maxGames = settings.maxGames;
    var as = $('autoScanToggle'); settings.autoScan = as ? as.checked : false;
    var th = $('themeSelect'); settings.theme = th ? th.value : 'dark';
    var ds = $('defaultSortSelect'); settings.defaultSort = ds ? ds.value : 'date';
    var cd = $('cacheDaysInput'); var cv = cd ? parseInt(cd.value, 10) : 0;
    settings.cacheDays = (isNaN(cv) || cv < 0) ? 0 : cv;
  }

  function sortHostNames(hostNamesArr) {
    var order = settings.hostOrder || ALL_HOSTS;
    var nameToId = {};
    for (var key in HOST_LABELS) nameToId[HOST_LABELS[key]] = key;
    hostNamesArr.sort(function (a, b) {
      var idA = nameToId[a] || a; var idB = nameToId[b] || b;
      var posA = order.indexOf(idA); if (posA === -1) posA = 999;
      var posB = order.indexOf(idB); if (posB === -1) posB = 999;
      return posA - posB;
    });
    return hostNamesArr;
  }

  async function loadFromStore() {
    try {
      var r = await Promise.all([window.api.store.get('gamesData'), window.api.store.get('favorites'), window.api.store.getSettings()]);
      gamesData = r[0] || {}; favorites = r[1] || [];
      var s = r[2] || {};
      settings.maxGames = s.maxGames || 0; settings.autoScan = s.autoScan || false;
      settings.theme = s.theme || 'dark'; settings.defaultSort = s.defaultSort || 'date';
      settings.cacheDays = s.cacheDays || 0;
      settings.hostOrder = (s.hostOrder && s.hostOrder.length > 0) ? s.hostOrder : ALL_HOSTS.slice();
      maxGames = settings.maxGames;
      sortByDate = settings.defaultSort === 'date';
      safeText(sortBtn, sortByDate ? 'Sort: Date' : 'Sort: Name');
      applyTheme(settings.theme); updateFavBtn();
      checkCacheExpiry();
    } catch (e) { console.error('Failed to load from store:', e); gamesData = {}; favorites = []; maxGames = 0; }
  }

  async function saveGamesToStore() {
    try { await window.api.store.set('gamesData', gamesData); } catch (e) { console.error('Failed to save games:', e); }
  }

  async function saveFavoritesToStore() {
    try { await window.api.store.set('favorites', favorites); } catch (e) { console.error('Failed to save favorites:', e); }
  }

  async function getGamesFromRSS() {
    var r = await window.api.fetchRSS(BASE_URL + RSS_PATH);
    if (r.success) return r.data; trackError(r.error); return {};
  }

  async function getGamesFromCategory() {
    var games = {}; var page = 1; var batchSize = 15; var consecutiveEmpty = 0; var hitEnd = false;
    while (true) {
      if (isCancelled) return games;
      var promises = [];
      for (var i = 0; i < batchSize; i++) {
        var p = page + i;
        var url = p === 1 ? BASE_URL + CATEGORY : BASE_URL + CATEGORY + 'page/' + p + '/';
        promises.push(window.api.fetchGameList(url, 8000));
      }
      var results = await Promise.allSettled(promises);
      if (isCancelled) return games;
      currentPagesScanned += batchSize; var foundAny = false;
      for (var r = 0; r < results.length; r++) {
        if (results[r].status === 'fulfilled') {
          var v = results[r].value;
          if (v.endOfList) { hitEnd = true; continue; }
          if (v.success && v.games && v.games.length > 0) {
            for (var g = 0; g < v.games.length; g++) {
              if (maxGames > 0 && Object.keys(games).length >= maxGames) { updateProgressStats(); return games; }
              var gm = v.games[g];
              var fu = gm.url.indexOf('http') === 0 ? gm.url : BASE_URL + gm.url;
              var fd = ''; try { fd = gm.date ? new Date(gm.date).toISOString().split('T')[0] : ''; } catch (e) { fd = gm.date || ''; }
              var cu = gm.cover ? (gm.cover.indexOf('http') === 0 ? gm.cover : BASE_URL + gm.cover) : '';
              games[gm.title] = { akira: [], viking: [], onefichier: [], other: [], cover: cu, voice: '', subtitles: '', notes: '', size: '', firmware: '', date: fd, url: fu, description: '', screenshots: [], password: '', screenLanguages: '', guide: '', cusa: null };
              currentGamesFound++;
            }
            foundAny = true;
          } else if (!v.success && v.error) { trackError(v.error); }
        }
      }
      updateProgressStats();
      if (hitEnd) break;
      if (!foundAny) { consecutiveEmpty++; if (consecutiveEmpty >= 2) break; } else { consecutiveEmpty = 0; }
      page += batchSize;
      await new Promise(function (res) { setTimeout(res, 50); });
    }
    return games;
  }

  var scrapingGames = {};

  async function scrapeGamePage(url, title) {
    if (scrapingGames[title]) return scrapingGames[title];
    var promise = (async function () {
      try {
        var r = await window.api.scrapeGamePage(url, title);
        if (r.success && r.data && gamesData[title]) {
          var d = r.data; var g = gamesData[title];
          g.akira = d.akira || []; g.viking = d.viking || []; g.onefichier = d.onefichier || []; g.other = d.other || [];
          g.cover = d.cover || g.cover; g.voice = d.voice || ''; g.subtitles = d.subtitles || '';
          g.notes = d.notes || ''; g.size = d.size || ''; g.firmware = d.firmware || '';
          g.date = d.date || g.date; g.description = cleanDescription(d.description || '');
          g.screenshots = d.screenshots || []; g.password = d.password || '';
          g.screenLanguages = d.screenLanguages || ''; g.guide = d.guide || ''; g.cusa = d.cusa || null;
          saveGamesToStore();
        } else if (!r.success) { return null; }
        return r.data || null;
      } catch (e) {
        console.error('scrapeGamePage error:', e);
        return null;
      } finally { delete scrapingGames[title]; }
    })();
    scrapingGames[title] = promise;
    return promise;
  }

  function createGameCard(title, cover) {
    var card = document.createElement('div'); card.className = 'game-card'; card.dataset.title = title;
    card.dataset.titleLower = title.toLowerCase();
    var star = document.createElement('div'); star.className = 'favorite-star'; star.textContent = '\u2605';
    if (favorites.indexOf(title) !== -1) star.classList.add('favorited');
    star.addEventListener('click', function (e) { e.stopPropagation(); toggleFavorite(title); star.classList.toggle('favorited'); });
    card.appendChild(star);
    if (cover) {
      var img = document.createElement('img');
      img.src = cover.indexOf('http') === 0 ? cover : BASE_URL + cover;
      img.alt = escapeHtml(title); img.className = 'game-cover';
      img.loading = 'lazy';
      img.onerror = function () { this.style.display = 'none'; };
      card.appendChild(img);
    }
    var t = document.createElement('div'); t.className = 'game-title'; safeText(t, title); card.appendChild(t);
    card.addEventListener('click', function () { if (!modalLoading) showModal(title); });
    return card;
  }

  function toggleFavorite(title) {
    var i = favorites.indexOf(title); if (i !== -1) favorites.splice(i, 1); else favorites.push(title);
    saveFavoritesToStore(); updateFavBtn();
  }

  function extractVersionDisplay(item, data, gameTitle) {
    // Build display: packDescription first, then fallback to CUSA + version + firmware
    var parts = [];

    // Pack description takes priority — it's the most informative
    var packDesc = item.packDescription || '';
    if (packDesc && packDesc.length > 3) {
      // If pack desc exists, use it as the main display text
      // But still add CUSA prefix if available
      var cm = null;
      if (data.cusa) {
        var cusaStr = String(data.cusa);
        cm = cusaStr.match(/(CUSA\d+)/);
      }
      if (!cm) cm = item.version.match(/(CUSA\d+)/);
      if (!cm) cm = gameTitle.match(/(CUSA\d+)/);
      if (cm) parts.push(cm[1]);
      parts.push(packDesc);
    } else {
      // No pack description — use CUSA + version + firmware
      var cm2 = item.version.match(/(CUSA\d+)/);
      if (!cm2 && data.cusa) {
        var cusaStr2 = String(data.cusa);
        cm2 = cusaStr2.match(/(CUSA\d+)/);
      }
      if (!cm2) cm2 = gameTitle.match(/(CUSA\d+)/);
      var code = cm2 ? cm2[1] : '';

      var vs = '';
      if (item.extractedVersion) { vs = 'v' + item.extractedVersion; }
      else { var vm = item.version.match(/[Vv]?(\d{1,2}[._]\d{2,3})/); if (vm) vs = 'v' + vm[1].replace('_', '.'); }

      var fw = '';
      if (item.extractedFirmware) {
        fw = (item.type === 'fix') ? 'Fix ' + formatFirmwareVersion(item.extractedFirmware) : 'FW ' + formatFirmwareVersion(item.extractedFirmware);
      }

      if (code) parts.push(code);
      if (vs) parts.push(vs);
      if (fw) parts.push(fw);
    }

    return parts.length > 0 ? parts.join(' \u00b7 ') : 'Download';
  }

  function buildDownloadLink(linkItem, type, data, gameTitle) {
    var row = document.createElement('div'); row.className = 'modal-link-item';
    var icon = document.createElement('span'); icon.className = 'modal-link-icon'; icon.textContent = '\u2193'; row.appendChild(icon);
    var displayName = extractVersionDisplay(linkItem, data, gameTitle);
    var txt = document.createElement('span'); txt.className = 'modal-link-text'; safeText(txt, displayName); row.appendChild(txt);
    var badge = document.createElement('span'); badge.className = 'modal-link-badge ' + type; safeText(badge, type); row.appendChild(badge);
    row.addEventListener('click', function () { safeOpenExternal(linkItem.link); });
    return row;
  }

  function showModalLoading(gameTitle) {
    modalBody.innerHTML = '';
    var loadWrap = document.createElement('div'); loadWrap.className = 'modal-loading';
    var spinner = document.createElement('div'); spinner.className = 'modal-loading-spinner'; loadWrap.appendChild(spinner);
    var loadText = document.createElement('div'); loadText.className = 'modal-loading-text';
    safeText(loadText, 'Loading ' + gameTitle + '...'); loadWrap.appendChild(loadText);
    modalBody.appendChild(loadWrap); openModal(modal);
  }

  function showModalError(gameTitle) {
    modalBody.innerHTML = '';
    var errWrap = document.createElement('div'); errWrap.className = 'modal-loading';
    var errIcon = document.createElement('div'); errIcon.className = 'modal-error-icon'; errIcon.textContent = '!'; errWrap.appendChild(errIcon);
    var errText = document.createElement('div'); errText.className = 'modal-loading-text';
    safeText(errText, 'Failed to load game details'); errWrap.appendChild(errText);
    var retryRow = document.createElement('div'); retryRow.style.cssText = 'margin-top:16px;display:flex;gap:10px;justify-content:center;';
    var retryModalBtn = document.createElement('button'); safeText(retryModalBtn, 'Retry');
    retryModalBtn.addEventListener('click', function () { showModal(gameTitle); }); retryRow.appendChild(retryModalBtn);
    var closeModalBtn = document.createElement('button'); safeText(closeModalBtn, 'Close'); closeModalBtn.style.opacity = '0.6';
    closeModalBtn.addEventListener('click', function () { closeModalFn(modal); }); retryRow.appendChild(closeModalBtn);
    errWrap.appendChild(retryRow); modalBody.appendChild(errWrap);
  }

  async function showModal(gameTitle) {
    if (modalLoading) return;
    modalLoading = true;
    try {
      var data = gamesData[gameTitle]; if (!data) return;
      var hasLinks = false;
      for (var hk = 0; hk < HOST_KEYS.length; hk++) { if (data[HOST_KEYS[hk]] && data[HOST_KEYS[hk]].length > 0) { hasLinks = true; break; } }
      if (!hasLinks && data.url) {
        showModalLoading(gameTitle);
        var result = await scrapeGamePage(data.url, gameTitle);
        if (!result) { showModalError(gameTitle); return; }
        data = gamesData[gameTitle]; if (!data) return;
      }
      renderModalContent(gameTitle, data);
    } finally {
      modalLoading = false;
    }
  }

  function renderModalContent(gameTitle, data) {
    modalBody.innerHTML = '';

    if (data.cover) {
      var cw = document.createElement('div'); cw.className = 'modal-cover-wrap';
      var ci = document.createElement('img'); ci.src = data.cover.indexOf('http') === 0 ? data.cover : BASE_URL + data.cover; ci.alt = escapeHtml(gameTitle);
      ci.onerror = function () { cw.style.display = 'none'; };
      cw.appendChild(ci);
      var covGr = document.createElement('div'); covGr.className = 'modal-cover-gradient'; cw.appendChild(covGr);
      modalBody.appendChild(cw);
    }

    var inner = document.createElement('div'); inner.className = 'modal-body-inner';
    var titleEl = document.createElement('h2'); titleEl.className = 'modal-game-title'; safeText(titleEl, gameTitle); inner.appendChild(titleEl);

    var bpFirmwares = getAllBackportFirmwares(data);
    var infoItems = [];
    if (data.firmware) infoItems.push({ label: 'Firmware', value: data.firmware, cls: 'firmware' });
    if (bpFirmwares.length === 1) infoItems.push({ label: 'Backport', value: bpFirmwares[0] + '.xx', cls: 'backport-fw' });
    else if (bpFirmwares.length > 1) infoItems.push({ label: 'Backports', value: bpFirmwares.map(function (n) { return n + '.xx'; }).join(', '), cls: 'backport-fw' });
    if (data.size) infoItems.push({ label: 'Size', value: data.size });
    if (data.voice) infoItems.push({ label: 'Voice', value: data.voice });
    if (data.subtitles) infoItems.push({ label: 'Subtitles', value: data.subtitles });
    if (data.screenLanguages) infoItems.push({ label: 'Languages', value: data.screenLanguages, full: true });
    if (data.notes) infoItems.push({ label: 'Notes', value: data.notes, full: true });

    if (infoItems.length > 0) {
      var grid = document.createElement('div'); grid.className = 'modal-info-grid';
      for (var ii = 0; ii < infoItems.length; ii++) {
        var infoItem = document.createElement('div'); infoItem.className = 'modal-info-item' + (infoItems[ii].full ? ' full-width' : '');
        var lbl = document.createElement('div'); lbl.className = 'modal-info-label'; safeText(lbl, infoItems[ii].label); infoItem.appendChild(lbl);
        var val = document.createElement('div'); val.className = 'modal-info-value' + (infoItems[ii].cls ? ' ' + infoItems[ii].cls : ''); safeText(val, infoItems[ii].value); infoItem.appendChild(val);
        grid.appendChild(infoItem);
      }
      inner.appendChild(grid);
    }

    if (data.description) {
      var cleanedDesc = cleanDescription(data.description);
      if (cleanedDesc) {
        var descSec = document.createElement('div'); descSec.className = 'modal-section';
        var descTitle = document.createElement('div'); descTitle.className = 'modal-section-title'; safeText(descTitle, 'Description'); descSec.appendChild(descTitle);
        var descBody = document.createElement('div'); descBody.className = 'modal-description'; descBody.innerHTML = formatDescription(cleanedDesc); descSec.appendChild(descBody);
        inner.appendChild(descSec);
      }
    }

    if (data.guide) {
      var guideSec = document.createElement('div'); guideSec.className = 'modal-section';
      var guideTitle = document.createElement('div'); guideTitle.className = 'modal-section-title'; safeText(guideTitle, 'Guide'); guideSec.appendChild(guideTitle);
      var guideBody = document.createElement('div'); guideBody.className = 'modal-description'; guideBody.innerHTML = '<p>' + escapeHtml(data.guide).replace(/\n/g, '<br>') + '</p>'; guideSec.appendChild(guideBody);
      inner.appendChild(guideSec);
    }

    if (data.screenshots && data.screenshots.length > 0) {
      var ssSec = document.createElement('div'); ssSec.className = 'modal-section';
      var ssTitle = document.createElement('div'); ssTitle.className = 'modal-section-title'; safeText(ssTitle, 'Screenshots'); ssSec.appendChild(ssTitle);
      var ssGrid = document.createElement('div'); ssGrid.className = 'modal-screenshots';
      data.screenshots.forEach(function (src) {
        var ssImg = document.createElement('img'); ssImg.src = src; ssImg.alt = 'Screenshot';
        ssImg.onerror = function () { this.style.display = 'none'; };
        ssImg.addEventListener('click', function () { fullImage.src = src; openModal(fullImageModal); });
        ssGrid.appendChild(ssImg);
      });
      ssSec.appendChild(ssGrid); inner.appendChild(ssSec);
    }

    var allLinks = [];
    for (var hki = 0; hki < HOST_KEYS.length; hki++) {
      var links = data[HOST_KEYS[hki]];
      if (!links) continue;
      for (var li = 0; li < links.length; li++) allLinks.push(links[li]);
    }

    var typeGroups = {};
    var backportFwGroups = {};
    for (var ai = 0; ai < allLinks.length; ai++) {
      var dl = allLinks[ai];
      var type = dl.type || 'game';
      var hostName = getHostDisplayName(dl);
      if (type === 'backport') {
        var fwGroup = getBackportFwGroup(dl);
        if (!backportFwGroups[fwGroup]) backportFwGroups[fwGroup] = {};
        if (!backportFwGroups[fwGroup][hostName]) backportFwGroups[fwGroup][hostName] = [];
        backportFwGroups[fwGroup][hostName].push(dl);
      } else {
        if (!typeGroups[type]) typeGroups[type] = {};
        if (!typeGroups[type][hostName]) typeGroups[type][hostName] = [];
        typeGroups[type][hostName].push(dl);
      }
    }

    var typeOrder = ['game', 'update', 'fix', 'dlc'];
    var typeKeys = typeOrder.filter(function (t) { return typeGroups[t]; });

    typeKeys.forEach(function (type) {
      var typeSec = document.createElement('div'); typeSec.className = 'modal-section';
      var typeTitleEl = document.createElement('div'); typeTitleEl.className = 'modal-section-title';
      safeText(typeTitleEl, type.charAt(0).toUpperCase() + type.slice(1) + ' Downloads'); typeSec.appendChild(typeTitleEl);
      var hostList = sortHostNames(Object.keys(typeGroups[type]));
      hostList.forEach(function (hn) {
        var hostLinks = typeGroups[type][hn];
        if (hostLinks.length === 0) return;
        var hg = document.createElement('div'); hg.className = 'modal-host-group';
        var hnEl = document.createElement('div'); hnEl.className = 'modal-host-name'; safeText(hnEl, hn); hg.appendChild(hnEl);
        hostLinks.forEach(function (linkItem) { hg.appendChild(buildDownloadLink(linkItem, type, data, gameTitle)); });
        typeSec.appendChild(hg);
      });
      inner.appendChild(typeSec);
    });

    var bpFwKeys = Object.keys(backportFwGroups).sort(function (a, b) { return parseInt(a) - parseInt(b); });
    if (bpFwKeys.length > 0) {
      bpFwKeys.forEach(function (fwKey) {
        var fwNum = parseInt(fwKey);
        var fwLabel = fwNum > 0 ? fwKey + '.xx' : '';
        var sectionTitle = fwLabel ? 'Backport ' + fwLabel + ' Downloads' : 'Backport Downloads';
        var typeSec = document.createElement('div'); typeSec.className = 'modal-section';
        var typeTitleEl = document.createElement('div'); typeTitleEl.className = 'modal-section-title'; safeText(typeTitleEl, sectionTitle); typeSec.appendChild(typeTitleEl);
        var hostList = sortHostNames(Object.keys(backportFwGroups[fwKey]));
        hostList.forEach(function (hn) {
          var hostLinks = backportFwGroups[fwKey][hn];
          if (hostLinks.length === 0) return;
          var hg = document.createElement('div'); hg.className = 'modal-host-group';
          var hnEl = document.createElement('div'); hnEl.className = 'modal-host-name'; safeText(hnEl, hn); hg.appendChild(hnEl);
          hostLinks.forEach(function (linkItem) { hg.appendChild(buildDownloadLink(linkItem, 'backport', data, gameTitle)); });
          typeSec.appendChild(hg);
        });
        inner.appendChild(typeSec);
      });
    }

    if (typeKeys.length === 0 && bpFwKeys.length === 0) {
      var noLinks = document.createElement('div'); noLinks.className = 'modal-no-links';
      safeText(noLinks, 'No download links available'); inner.appendChild(noLinks);
    }

    modalBody.appendChild(inner);
    openModal(modal);
    modalBody.parentElement.scrollTop = 0;
  }

  function buildFuseIndex(data) {
    var keys = Object.keys(data);
    if (keys.length === lastFuseDataCount) return;
    lastFuseDataCount = keys.length;
    var searchData = keys.map(function (title) {
      var g = data[title] || {};
      return { title: title, searchable: [title, g.voice || '', g.subtitles || '', g.notes || '', g.size || '', g.firmware || '', g.screenLanguages || '', g.guide || ''].join(' ').toLowerCase() };
    });
    if (typeof Fuse !== 'undefined') {
      fuseInstance = new Fuse(searchData, { keys: ['title', 'searchable'], threshold: 0.3, includeScore: true });
    } else {
      fuseInstance = { search: function (q) { var ql = q.toLowerCase(); return searchData.filter(function (it) { return it.title.toLowerCase().indexOf(ql) !== -1 || it.searchable.indexOf(ql) !== -1; }).map(function (it) { return { item: it, score: 0 }; }); } };
    }
  }

  function displayResults(data) {
    data = data || gamesData;
    if (resultsDiv) resultsDiv.innerHTML = '';
    var filtered = {};
    var entries = Object.entries(data);
    for (var fi = 0; fi < entries.length; fi++) {
      if (!showFavoritesOnly || favorites.indexOf(entries[fi][0]) !== -1) {
        filtered[entries[fi][0]] = entries[fi][1];
      }
    }
    var sorted = sortByDate
      ? Object.entries(filtered).sort(function (a, b) { return new Date(b[1].date || '1970-01-01') - new Date(a[1].date || '1970-01-01'); })
      : Object.entries(filtered).sort(function (a, b) { return a[0].localeCompare(b[0]); });
    buildFuseIndex(data);
    var fragment = document.createDocumentFragment();
    for (var i = 0; i < sorted.length; i++) {
      fragment.appendChild(createGameCard(sorted[i][0], sorted[i][1].cover));
    }
    resultsDiv.appendChild(fragment);
  }

  function filterCardsInPlace(query) {
    if (!resultsDiv) return;
    var cards = resultsDiv.children;
    if (!query) {
      for (var i = 0; i < cards.length; i++) { cards[i].style.display = ''; }
      return;
    }
    if (fuseInstance) {
      var results = fuseInstance.search(query);
      var matchSet = {};
      for (var r = 0; r < results.length; r++) {
        if (results[r].item) matchSet[results[r].item.title] = true;
      }
      for (var c = 0; c < cards.length; c++) {
        var title = cards[c].dataset.title;
        cards[c].style.display = matchSet[title] ? '' : 'none';
      }
    } else {
      var ql = query.toLowerCase();
      for (var j = 0; j < cards.length; j++) {
        var tl = cards[j].dataset.titleLower || '';
        cards[j].style.display = tl.indexOf(ql) !== -1 ? '' : 'none';
      }
    }
  }

  async function displayCachedResults() {
    await loadFromStore();
    var c = Object.keys(gamesData).length;
    if (c > 0) { displayResults(); safeText(statusDiv, 'Loaded ' + c + ' games from cache'); }
    else { safeText(statusDiv, 'No cached results \u2014 Click "Start" to begin'); }
  }

  async function runScraper() {
    if (isScraping) return;
    isScraping = true;
    var startTime = Date.now();
    isCancelled = false; resetFetchErrors(); setButtonsDuringScan(true); hideRetry();
    clearSearchInput();
    safeText(statusDiv, 'Scanning...');
    if (resultsDiv) resultsDiv.innerHTML = '';
    currentGamesFound = 0; currentPagesScanned = 0; showProgress();
    lastFuseDataCount = -1;

    var rssPromise = getGamesFromRSS();
    var games = await getGamesFromCategory();
    var rssData = await rssPromise;

    if (isCancelled) { isScraping = false; setButtonsDuringScan(false); hideProgress(); return; }

    var finalGames = games;
    if (Object.keys(games).length === 0) {
      finalGames = {};
      Object.entries(rssData).forEach(function (e) {
        finalGames[e[0]] = { akira: [], viking: [], onefichier: [], other: [], cover: e[1].cover, voice: '', subtitles: '', notes: '', size: '', firmware: '', date: e[1].date, url: e[1].url, description: '', screenshots: [], password: '', screenLanguages: '', guide: '', cusa: null };
      });
    } else {
      Object.keys(rssData).forEach(function (k) {
        if (finalGames[k]) {
          if (!finalGames[k].cover && rssData[k].cover) finalGames[k].cover = rssData[k].cover;
          if (!finalGames[k].date && rssData[k].date) finalGames[k].date = rssData[k].date;
        }
      });
    }

    var total = Object.keys(finalGames).length;
    if (total === 0) {
      showNotification('No games found', 'error'); isScraping = false; setButtonsDuringScan(false); showRetry();
      safeText(statusDiv, 'No games found'); hideProgress(); return;
    }

    var dur = Math.round((Date.now() - startTime) / 1000);
    completeProgress(total);
    var msg = 'Found ' + total + ' games in ' + dur + 's';
    if (maxGames > 0 && total >= maxGames) msg = 'Found ' + total + ' games (limit ' + maxGames + ') in ' + dur + 's';
    var es = getErrorSummary();
    if (fetchErrors.total > 0) { msg += ' \u2014 Warning: ' + es; showNotification('Warnings: ' + es, fetchErrors.total > 10 ? 'error' : 'info'); }

    safeText(statusDiv, msg);
    gamesData = finalGames; displayResults();
    await saveGamesToStore();
    safeDesktopNotification('Scan Complete', 'Found ' + total + ' games in ' + dur + 's');
    hideProgress(); isScraping = false; setButtonsDuringScan(false);
  }

  function cancelScan() {
    isCancelled = true; isScraping = false; setButtonsDuringScan(false);
    safeText(statusDiv, 'Scan cancelled');
    safeDesktopNotification('Scan Cancelled', 'Scraping was cancelled'); hideProgress();
  }

  async function clearCache() {
    if (window.confirm('Clear all cached games and favorites?')) {
      await window.api.store.delete('gamesData');
      await window.api.store.delete('favorites');
      favorites = []; gamesData = {}; lastFuseDataCount = -1; fuseInstance = null;
      if (resultsDiv) resultsDiv.innerHTML = '';
      clearSearchInput();
      showFavoritesOnly = false;
      safeText(statusDiv, 'Cache cleared'); updateFavBtn();
    }
  }

  function toggleSort() {
    sortByDate = !sortByDate;
    safeText(sortBtn, sortByDate ? 'Sort: Date' : 'Sort: Name');
    displayResults();
    reapplySearch();
  }

  function toggleFavorites() {
    showFavoritesOnly = !showFavoritesOnly;
    displayResults(); updateFavBtn();
    reapplySearch();
  }

  function openSettings() { populateSettingsUI(); openModal(settingsModal); }

  async function saveSettings() {
    collectSettings(); applyTheme(settings.theme);
    sortByDate = settings.defaultSort === 'date';
    safeText(sortBtn, sortByDate ? 'Sort: Date' : 'Sort: Name');
    await window.api.store.setSettings(settings);
    showNotification('Settings saved', 'success');
    closeModalFn(settingsModal); displayResults();
    reapplySearch();
  }

  displayCachedResults().then(function () {
    if (settings.autoScan && Object.keys(gamesData).length === 0) runScraper();
  });
  setButtonsDuringScan(false); hideRetry();

  if (discordLink) { discordLink.onclick = function (e) { e.preventDefault(); e.stopPropagation(); safeOpenExternal(DISCORD_URL); return false; }; }

  if (searchInput) {
    searchInput.addEventListener('input', function () {
      if (searchDebounceTimer) clearTimeout(searchDebounceTimer);
      searchDebounceTimer = setTimeout(function () {
        var q = searchInput.value.trim();
        filterCardsInPlace(q);
      }, 120);
    });
  }

  if (closeBtn) closeBtn.addEventListener('click', function () { closeModalFn(modal); });
  if (fullImageClose) fullImageClose.addEventListener('click', function () { closeModalFn(fullImageModal); });

  window.addEventListener('click', function (e) {
    if (e.target === modal) closeModalFn(modal);
    if (e.target === fullImageModal) closeModalFn(fullImageModal);
    if (settingsModal && e.target === settingsModal) closeModalFn(settingsModal);
  });

  window.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
      if (fullImageModal && fullImageModal.style.display === 'block') { closeModalFn(fullImageModal); return; }
      if (settingsModal && settingsModal.style.display === 'block') { closeModalFn(settingsModal); return; }
      if (modal && modal.style.display === 'block') { closeModalFn(modal); return; }
    }
  });

  if (scrapeBtn) scrapeBtn.addEventListener('click', function () { runScraper(); });
  if (cancelBtn) cancelBtn.addEventListener('click', cancelScan);
  if (clearCacheBtn) clearCacheBtn.addEventListener('click', clearCache);
  if (retryBtn) retryBtn.addEventListener('click', function () { hideRetry(); runScraper(); });
  if (sortBtn) sortBtn.addEventListener('click', toggleSort);
  if (favBtn) favBtn.addEventListener('click', toggleFavorites);
  if (settingsBtn) settingsBtn.addEventListener('click', openSettings);
  if (saveSettingsBtn) saveSettingsBtn.addEventListener('click', saveSettings);
  if (settingsClose) settingsClose.addEventListener('click', function () { closeModalFn(settingsModal); });
});