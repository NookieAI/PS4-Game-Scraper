const { shell } = require('electron');

document.addEventListener('DOMContentLoaded', () => {
  const cheerio = require('cheerio');
  const axios = require('axios');

  const scrapeBtn = document.getElementById('scrapeBtn');
  const cancelBtn = document.getElementById('cancelBtn');
  const clearCacheBtn = document.getElementById('clearCacheBtn');
  const retryBtn = document.getElementById('retryBtn');
  const sortBtn = document.getElementById('sortBtn');
  const favBtn = document.getElementById('favBtn');
  const statusDiv = document.getElementById('status');
  const progressContainer = document.getElementById('progressContainer');
  const progressBar = document.getElementById('progressBar');
  const progressText = document.getElementById('progressText');
  const gameCountDiv = document.getElementById('gameCount');
  const resultsDiv = document.getElementById('results');
  const searchInput = document.getElementById('searchInput');
  const notificationDiv = document.getElementById('notification');
  const discordLink = document.getElementById('discord-link');
  const modal = document.getElementById('modal');
  const modalBody = document.getElementById('modal-body');
  const closeBtn = document.querySelector('#modal .close');
  const fullImageModal = document.getElementById('fullImageModal');
  const fullImage = document.getElementById('fullImage');
  const fullImageClose = document.querySelector('#fullImageModal .close');

  const BASE_URL = 'https://dlpsgame.com';
  let isCancelled = false;
  let gamesData = {};
  let sortByDate = true; // true for date, false for name
  let showFavoritesOnly = false; // true to show only favorites
  let favorites = JSON.parse(localStorage.getItem('favorites')) || [];
  let currentGamesFound = 0; // Counter for games found during scraping (for progress updates)

  // Ensure progress bar and text are hidden on load
  if (progressContainer) progressContainer.style.visibility = 'hidden';
  if (progressText) {
    progressText.textContent = '';
    progressText.style.visibility = 'hidden';
  }

  console.log('Renderer loaded');
  console.log('scrapeBtn:', scrapeBtn); // Debug: Check if button is found

  if (!scrapeBtn) {
    console.error('Scrape button not found. Check HTML for id="scrapeBtn".');
    return;
  }

  // Set favBtn to star icon initially
  if (favBtn) {
    favBtn.textContent = '★';
    favBtn.style.fontSize = '18px';
    favBtn.style.color = '#ccc';
    favBtn.style.width = 'auto';
    favBtn.style.padding = '5px 10px';
    favBtn.style.background = 'none';
    favBtn.style.border = 'none';
    favBtn.style.boxShadow = 'none';
  }

  // Function to show notifications (only for errors now)
  function showNotification(message, type = 'info') {
    notificationDiv.textContent = message;
    notificationDiv.style.background = type === 'error' ? '#e74c3c' : type === 'success' ? '#27ae60' : '#3498db';
    notificationDiv.style.display = 'block';
    setTimeout(() => {
      notificationDiv.style.display = 'none';
    }, 5000);
  }

  // Function to show desktop notifications
  function showDesktopNotification(title, body) {
    if (Notification.permission === 'granted') {
      new Notification(title, { body: body });
    } else if (Notification.permission !== 'denied') {
      Notification.requestPermission().then(permission => {
        if (permission === 'granted') {
          new Notification(title, { body: body });
        }
      });
    }
  }

  // Function to show status with auto-hide
  function showStatus(message) {
    statusDiv.textContent = message;
    statusDiv.classList.add('show');
    setTimeout(() => {
      statusDiv.classList.remove('show');
    }, 5000);
  }

  // Function to disable/enable buttons during scan (only Cancel enabled during scan)
  function setButtonsDuringScan(scanning) {
    scrapeBtn.disabled = scanning;
    cancelBtn.disabled = !scanning;
    cancelBtn.style.display = scanning ? 'inline-block' : 'none';
    clearCacheBtn.disabled = scanning;
    retryBtn.disabled = scanning;
    if (sortBtn) sortBtn.disabled = scanning;
    if (favBtn) favBtn.disabled = scanning;
    searchInput.disabled = scanning;
  }

  // Function to fetch games from RSS feed (supplemental data)
  async function getGamesFromRSS() {
    try {
      const rssUrl = `${BASE_URL}/category/ps4/feed/`;
      const response = await axios.get(rssUrl, { timeout: 10000 });
      const $ = cheerio.load(response.data, { xmlMode: true });
      const rssData = {};
      $('item').each((i, el) => {
        const title = $(el).find('title').text().trim();
        const link = $(el).find('link').text().trim();
        const pubDate = $(el).find('pubDate').text().trim();
        const cover = $(el).find('enclosure').attr('url') || '';
        const date = new Date(pubDate).toISOString().split('T')[0];
        if (title && link) {
          rssData[title] = { cover: cover, date: date, url: link };
        }
      });
      console.log('RSS data:', Object.keys(rssData).length);
      if (progressText) progressText.textContent = `Fetching game list... RSS: ${Object.keys(rssData).length} items. Games: ${currentGamesFound} found so far.`;
      return rssData;
    } catch (error) {
      console.error('RSS fetch failed:', error);
      return {};
    }
  }

  // Function to get all games from category pages (full list with dates/covers) - optimized for speed with concurrent batch fetching
  async function getGamesFromCategory() {
    const games = {};
    let page = 1;
    const batchSize = 5; // Fetch 5 pages concurrently
    while (true) {
      const promises = [];
      for (let i = 0; i < batchSize; i++) {
        const currentPage = page + i;
        const pageUrl = currentPage === 1 ? `${BASE_URL}/category/ps4/` : `${BASE_URL}/category/ps4/page/${currentPage}/`;
        promises.push(axios.get(pageUrl, { timeout: 10000 }).then(response => ({ page: currentPage, response })).catch(() => null));
      }
      const results = await Promise.allSettled(promises);
      let foundAny = false;
      for (const result of results) {
        if (result.status === 'fulfilled' && result.value) {
          const { page: currentPage, response } = result.value;
          const $ = cheerio.load(response.data);
          let found = false;
          $('.post').each((i, el) => {
            const $post = $(el);
            const title = $post.find('h2 a').text().trim() || $post.find('h2 a').attr('title') || '';
            const url = $post.find('h2 a').attr('href');
            const date = $post.find('.publish-date, time').text().trim() || $post.find('.publish-date, time').attr('datetime') || '';
            const cover = $post.find('img').attr('src') || '';
            if (title && url) {
              const cleanTitle = title.replace(/\s+/g, ' ').trim();
              const fullUrl = url.startsWith('http') ? url : `${BASE_URL}${url}`;
              const formattedDate = date ? new Date(date).toISOString().split('T')[0] : '';
              games[cleanTitle] = { akira: [], viking: [], mediafire: [], onefile: [], cover: cover.startsWith('http') ? cover : `${BASE_URL}${cover}`, voice: '', subtitles: '', notes: '', size: '', date: formattedDate, url: fullUrl, description: '', screenshots: [] };
              found = true;
              currentGamesFound++; // Increment counter
            }
          });
          if (found) foundAny = true;
        }
      }
      if (!foundAny) break;
      if (progressText) progressText.textContent = `Fetching game list... Found ${currentGamesFound} games so far.`;
      page += batchSize;
      await new Promise(resolve => setTimeout(resolve, 100)); // Increased delay
    }
    console.log('Games from category:', Object.keys(games).length);
    return games;
  }

  // Function to scrape a game page for Akira and Viking download links, description, and screenshots (lazy load)
  async function scrapeGamePage(gameUrl, gameTitle) {
    try {
      const response = await axios.get(gameUrl, { timeout: 10000 });
      const $ = cheerio.load(response.data);
      // Collect download links by host
      const akiraLinks = [];
      const vikingLinks = [];
      const mediafireLinks = [];
      const onefileLinks = [];
      $('a[href]').each((i, el) => {
        const href = $(el).attr('href');
        if (href) {
          const versionText = $(el).closest('div').text().trim().replace(/\s+/g, ' ') || $(el).closest('p').text().trim().replace(/\s+/g, ' ') || $(el).text().trim() || 'Download Link';
          const linkData = { link: href, version: versionText };
          if (href.includes('akirabox.com')) {
            akiraLinks.push(linkData);
          } else if (href.includes('vikingfile.com')) {
            vikingLinks.push(linkData);
          } else if (href.includes('mediafire.com')) {
            // Clean MediaFire link by removing trailing '/file'
            linkData.link = href.replace(/\/file$/, '');
            mediafireLinks.push(linkData);
          } else if (href.includes('1file.com')) {
            onefileLinks.push(linkData);
          }
        }
      });
      const title = $('title').text() || '';
      const metaDesc = $('meta[name="description"]').attr('content') || '';
      const cover = $('meta[property="og:image"]').attr('content') || '';
      const voice = title.match(/Voice\s*:\s*([^N]+)/)?.[1]?.trim() || '';
      const subtitles = title.match(/Subtitles?\s*:\s*([^N]+)/)?.[1]?.trim() || '';
      const notes = title.match(/Note\s*:\s*(.+?)(?:\(|$)/)?.[1]?.trim() || '';
      const size = title.match(/Size\s*:\s*([^N]+)/)?.[1]?.trim() || '';
      const date = $('meta[property="article:published_time"]').attr('content') || $('time').attr('datetime') || '';
      
      // Extract description from blockquote or first p or meta
      const description = $('.entry-content blockquote').text().trim() || $('.entry-content p').first().text().trim() || metaDesc || '';
      
      // Extract screenshots (images within the content, excluding cover, limit to 2)
      const screenshots = [];
      let screenshotCount = 0;
      $('.entry-content img').each((i, el) => {
        if (screenshotCount >= 2) return false;
        const src = $(el).attr('src');
        if (src && !src.includes('cover') && !src.includes('thumbnail') && src !== cover) {
          screenshots.push(src.startsWith('http') ? src : `${BASE_URL}${src}`);
          screenshotCount++;
        }
      });
      
      // Update gamesData with full details
      if (gamesData[gameTitle]) {
        gamesData[gameTitle].akira = akiraLinks;
        gamesData[gameTitle].viking = vikingLinks;
        gamesData[gameTitle].mediafire = mediafireLinks;
        gamesData[gameTitle].onefile = onefileLinks;
        gamesData[gameTitle].cover = cover || gamesData[gameTitle].cover;
        gamesData[gameTitle].voice = voice;
        gamesData[gameTitle].subtitles = subtitles;
        gamesData[gameTitle].notes = notes;
        gamesData[gameTitle].size = size;
        gamesData[gameTitle].date = date || gamesData[gameTitle].date;
        gamesData[gameTitle].description = description;
        gamesData[gameTitle].screenshots = screenshots;
        try {
          localStorage.setItem('gamesData', JSON.stringify(gamesData));
        } catch (storageError) {
          console.warn('Failed to save to localStorage:', storageError);
        }
      }
      
      return { akira: akiraLinks, viking: vikingLinks, mediafire: mediafireLinks, onefile: onefileLinks, title, metaDesc, cover, voice, subtitles, notes, size, date, description, screenshots };
    } catch (error) {
      console.error('Error scraping game page:', error);
      showNotification('Failed to load game details.', 'error');
      return { akira: [], viking: [], mediafire: [], onefile: [], title: '', metaDesc: '', cover: '', voice: '', subtitles: '', notes: '', size: '', date: '', description: '', screenshots: [] };
    }
  }

  // Function to create a game card in the UI
  function createGameCard(gameTitle, cover, voice, subtitles, notes, size) {
    const card = document.createElement('div');
    card.className = 'game-card';
    card.dataset.title = gameTitle;
    
    const star = document.createElement('div');
    star.className = 'favorite-star';
    star.textContent = '★';
    if (favorites.includes(gameTitle)) star.classList.add('favorited');
    star.addEventListener('click', (e) => {
      e.stopPropagation();
      toggleFavorite(gameTitle);
      star.classList.toggle('favorited');
    });
    card.appendChild(star);
    
    if (cover) {
      const img = document.createElement('img');
      img.src = cover.startsWith('http') ? cover : `${BASE_URL}${cover}`;
      img.alt = gameTitle;
      img.className = 'game-cover';
      card.appendChild(img);
    }
    
    const title = document.createElement('div');
    title.className = 'game-title';
    title.textContent = gameTitle;
    card.appendChild(title);
    
    const details = document.createElement('div');
    details.className = 'game-details';
    let detailsText = '';
    if (voice) detailsText += `Voice: ${voice} | `;
    if (subtitles) detailsText += `Subtitles: ${subtitles} | `;
    if (notes) detailsText += `Notes: ${notes} | `;
    if (size) detailsText += `Size: ${size}`;
    details.textContent = detailsText.replace(/ \| $/, '');
    if (detailsText) card.appendChild(details);
    
    card.addEventListener('click', () => showModal(gameTitle));
    
    return card;
  }

  // Function to toggle favorite
  function toggleFavorite(gameTitle) {
    if (favorites.includes(gameTitle)) {
      favorites = favorites.filter(fav => fav !== gameTitle);
    } else {
      favorites.push(gameTitle);
    }
    localStorage.setItem('favorites', JSON.stringify(favorites));
  }

  // Function to format description with headings or bullets
  function formatDescription(text) {
    if (text.includes('•')) {
      // Format as bullet list
      const parts = text.split('•');
      let html = '<p>' + parts[0].trim() + '</p>';
      if (parts.length > 1) {
        html += '<ul>';
        for (let i = 1; i < parts.length; i++) {
          html += '<li>' + parts[i].trim() + '</li>';
        }
        html += '</ul>';
      }
      return html;
    } else {
      // Format with headings
      const headings = [
        'RIDE VEHICLES FROM BEYOND YOUR IMAGINATION',
        'UPGRADE AND CUSTOMIZE YOUR EXPERIENCE',
        'EXPLORE BEYOND THE DUNES',
        'BECOME THE SAVIOR THE WORLD NEEDS'
      ];
      let formatted = text;
      headings.forEach(heading => {
        formatted = formatted.replace(heading, '</p><h4>' + heading + '</h4><p>');
      });
      formatted = '<p>' + formatted + '</p>';
      formatted = formatted.replace('<p></p><h4>', '<h4>');
      return formatted;
    }
  }

  // Function to show modal with game details, links, description, and screenshots
  async function showModal(gameTitle) {
    const data = gamesData[gameTitle];
    if (!data) return;
    
    if (data.akira.length === 0 && data.viking.length === 0 && data.mediafire.length === 0 && data.onefile.length === 0 && data.url) {
      showNotification('Loading game details...', 'info');
      await scrapeGamePage(data.url, gameTitle);
      // Refresh data after scraping
      const updatedData = gamesData[gameTitle];
      if (updatedData) Object.assign(data, updatedData);
    }
    
    modalBody.innerHTML = '';
    
    // Cover
    if (data.cover) {
      const img = document.createElement('img');
      img.src = data.cover.startsWith('http') ? data.cover : `${BASE_URL}${data.cover}`;
      img.alt = gameTitle;
      img.className = 'game-cover';
      modalBody.appendChild(img);
    }
    
    // Details
    const details = document.createElement('div');
    details.className = 'game-details';
    let detailsText = '';
    if (data.voice) detailsText += `Voice: ${data.voice} | `;
    if (data.subtitles) detailsText += `Subtitles: ${data.subtitles} | `;
    if (data.notes) detailsText += `Notes: ${data.notes} | `;
    if (data.size) detailsText += `Size: ${data.size}`;
    details.textContent = detailsText.replace(/ \| $/, '');
    if (detailsText) modalBody.appendChild(details);
    
    // Description
    if (data.description) {
      const descSection = document.createElement('div');
      descSection.className = 'description-section';
      descSection.innerHTML = '<h3>Description:</h3>' + formatDescription(data.description);
      modalBody.appendChild(descSection);
    }
    
    // Screenshots
    if (data.screenshots && data.screenshots.length > 0) {
      const screenshotsSection = document.createElement('div');
      screenshotsSection.className = 'screenshots-section';
      screenshotsSection.innerHTML = '<h3>Screenshots:</h3>';
      const gallery = document.createElement('div');
      gallery.className = 'screenshot-gallery';
      data.screenshots.forEach(src => {
        const img = document.createElement('img');
        img.src = src;
        img.alt = 'Screenshot';
        img.className = 'screenshot';
        img.addEventListener('click', () => {
          fullImage.src = src;
          fullImageModal.style.display = 'block';
        });
        gallery.appendChild(img);
      });
      screenshotsSection.appendChild(gallery);
      modalBody.appendChild(screenshotsSection);
    }
    
    // Akira Links
    if (data.akira && data.akira.length > 0) {
      const akiraSection = document.createElement('div');
      akiraSection.className = 'links-section';
      akiraSection.innerHTML = '<h3>Akira Links:</h3>';
      const akiraList = document.createElement('ul');
      akiraList.className = 'link-list';
      data.akira.forEach(item => {
        const li = document.createElement('li');
        const a = document.createElement('a');
        a.href = item.link;
        let displayText = '';
        const ppsaMatch = item.version.match(/PPSA\d+ – [A-Z]{3}/g);
        if (ppsaMatch) {
          const ppsaStr = ppsaMatch.join(' ').replace(/ – /g, ' (') + ')';
          displayText += ppsaStr + ' - ';
        }
        const versionMatch = item.version.match(/\( ?v[\d.]+\)/);
        if (versionMatch) displayText += versionMatch[0].replace(' ', '');
        a.textContent = displayText || item.link;
        a.addEventListener('click', (e) => {
          e.preventDefault();
          shell.openExternal(item.link);
        });
        li.appendChild(a);
        akiraList.appendChild(li);
      });
      akiraSection.appendChild(akiraList);
      modalBody.appendChild(akiraSection);
    } else {
      const noAkira = document.createElement('p');
      noAkira.textContent = 'No Akira links available yet. Try refreshing the modal.';
      modalBody.appendChild(noAkira);
    }
    
    // Viking Links
    if (data.viking && data.viking.length > 0) {
      const vikingSection = document.createElement('div');
      vikingSection.className = 'links-section';
      vikingSection.innerHTML = '<h3>Viking Links:</h3>';
      const vikingList = document.createElement('ul');
      vikingList.className = 'link-list';
      data.viking.forEach(item => {
        const li = document.createElement('li');
        const a = document.createElement('a');
        a.href = item.link;
        let displayText = '';
        const ppsaMatch = item.version.match(/PPSA\d+ – [A-Z]{3}/g);
        if (ppsaMatch) {
          const ppsaStr = ppsaMatch.join(' ').replace(/ – /g, ' (') + ')';
          displayText += ppsaStr + ' - ';
        }
        const versionMatch = item.version.match(/\( ?v[\d.]+\)/);
        if (versionMatch) displayText += versionMatch[0].replace(' ', '');
        a.textContent = displayText || item.link;
        a.addEventListener('click', (e) => {
          e.preventDefault();
          shell.openExternal(item.link);
        });
        li.appendChild(a);
        vikingList.appendChild(li);
      });
      vikingSection.appendChild(vikingList);
      modalBody.appendChild(vikingSection);
    } else {
      const noViking = document.createElement('p');
      noViking.textContent = 'No Viking links available yet. Try refreshing the modal.';
      modalBody.appendChild(noViking);
    }
    
    // MediaFire Links
    if (data.mediafire && data.mediafire.length > 0) {
      const mediafireSection = document.createElement('div');
      mediafireSection.className = 'links-section';
      mediafireSection.innerHTML = '<h3>MediaFire Links:</h3>';
      const mediafireList = document.createElement('ul');
      mediafireList.className = 'link-list';
      data.mediafire.forEach(item => {
        const li = document.createElement('li');
        const a = document.createElement('a');
        a.href = item.link;
        let displayText = '';
        const ppsaMatch = item.version.match(/PPSA\d+ – [A-Z]{3}/g);
        if (ppsaMatch) {
          const ppsaStr = ppsaMatch.join(' ').replace(/ – /g, ' (') + ')';
          displayText += ppsaStr + ' - ';
        }
        const versionMatch = item.version.match(/\( ?v[\d.]+\)/);
        if (versionMatch) displayText += versionMatch[0].replace(' ', '');
        // Display short MediaFire link
        a.textContent = displayText + item.link.replace(/\/[^\/]+$/, '') || item.link;
        a.addEventListener('click', (e) => {
          e.preventDefault();
          shell.openExternal(item.link);
        });
        li.appendChild(a);
        mediafireList.appendChild(li);
      });
      mediafireSection.appendChild(mediafireList);
      modalBody.appendChild(mediafireSection);
    } else {
      const noMediafire = document.createElement('p');
      noMediafire.textContent = 'No MediaFire links available yet. Try refreshing the modal.';
      modalBody.appendChild(noMediafire);
    }
    
    // OneFile Links
    if (data.onefile && data.onefile.length > 0) {
      const onefileSection = document.createElement('div');
      onefileSection.className = 'links-section';
      onefileSection.innerHTML = '<h3>OneFile Links:</h3>';
      const onefileList = document.createElement('ul');
      onefileList.className = 'link-list';
      data.onefile.forEach(item => {
        const li = document.createElement('li');
        const a = document.createElement('a');
        a.href = item.link;
        let displayText = '';
        const ppsaMatch = item.version.match(/PPSA\d+ – [A-Z]{3}/g);
        if (ppsaMatch) {
          const ppsaStr = ppsaMatch.join(' ').replace(/ – /g, ' (') + ')';
          displayText += ppsaStr + ' - ';
        }
        const versionMatch = item.version.match(/\( ?v[\d.]+\)/);
        if (versionMatch) displayText += versionMatch[0].replace(' ', '');
        a.textContent = displayText || item.link;
        a.addEventListener('click', (e) => {
          e.preventDefault();
          shell.openExternal(item.link);
        });
        li.appendChild(a);
        onefileList.appendChild(li);
      });
      onefileSection.appendChild(onefileList);
      modalBody.appendChild(onefileSection);
    } else {
      const noOnefile = document.createElement('p');
      noOnefile.textContent = 'No OneFile links available yet. Try refreshing the modal.';
      modalBody.appendChild(noOnefile);
    }
    
    modal.style.display = 'block';
  }

  // Function to display results sorted by date or name, filtered by favorites if needed
  function displayResults(data = gamesData) {
    resultsDiv.innerHTML = '';
    let filteredData = showFavoritesOnly ? Object.fromEntries(Object.entries(data).filter(([title]) => favorites.includes(title))) : data;
    let sorted;
    if (sortByDate) {
      sorted = Object.entries(filteredData).sort(([a, adata], [b, bdata]) => {
        const dateA = new Date(adata.date || '1970-01-01');
        const dateB = new Date(bdata.date || '1970-01-01');
        return dateB - dateA;
      });
    } else {
      sorted = Object.entries(filteredData).sort(([a], [b]) => a.localeCompare(b));
    }
    
    for (const [gameTitle, links] of sorted) {
      const card = createGameCard(gameTitle, links.cover, links.voice, links.subtitles, links.notes, links.size);
      resultsDiv.appendChild(card);
    }
  }

  // Function to display cached results
  function displayCachedResults() {
    const cachedData = localStorage.getItem('gamesData');
    if (cachedData) {
      gamesData = JSON.parse(cachedData);
      displayResults();
      const total = Object.keys(gamesData).length;
      if (statusDiv) statusDiv.textContent = `Loaded ${total} games from cache. Click "Start" to begin.`;
      if (gameCountDiv) {
        gameCountDiv.textContent = `Found ${total} games`;
        gameCountDiv.style.display = 'block';
      }
      console.log('Displayed cached results');
    } else {
      if (statusDiv) statusDiv.textContent = 'No cached results. Click "Start" to begin.';
      if (gameCountDiv) gameCountDiv.style.display = 'none';
    }
  }

  // Main scraping function (fast with category + RSS enrichment)
  async function runScraper() {
    console.log('Starting fast scraper');
    const startTime = Date.now();
    isCancelled = false;
    setButtonsDuringScan(true);
    retryBtn.style.display = 'none';
    if (statusDiv) statusDiv.textContent = 'Starting scan...';
    // Show progress bar with pulse-wiggle animation
    if (progressContainer) {
      progressContainer.style.visibility = 'visible';
    }
    if (progressBar) {
      progressBar.classList.add('indeterminate');
    }
    if (progressText) {
      progressText.style.visibility = 'visible';
      progressText.textContent = 'Fetching game list...';
    }
    if (resultsDiv) resultsDiv.innerHTML = '';
    if (gameCountDiv) gameCountDiv.style.display = 'none';
    
    currentGamesFound = 0; // Reset counter
    
    // Get full list from category pages and RSS concurrently
    const [games, rssData] = await Promise.all([getGamesFromCategory(), getGamesFromRSS()]);
    
    let finalGames = games;
    if (Object.keys(games).length === 0) {
      // Fallback to RSS if category fails
      finalGames = {};
      for (const [title, data] of Object.entries(rssData)) {
        finalGames[title] = { akira: [], viking: [], mediafire: [], onefile: [], cover: data.cover, voice: '', subtitles: '', notes: '', size: '', date: data.date, url: data.url, description: '', screenshots: [] };
      }
    } else {
      // Enrich with RSS data if available
      for (const [title, rssInfo] of Object.entries(rssData)) {
        if (finalGames[title]) {
          if (!finalGames[title].cover && rssInfo.cover) finalGames[title].cover = rssInfo.cover;
          if (!finalGames[title].date && rssInfo.date) finalGames[title].date = rssInfo.date;
        }
      }
    }
    
    const totalGames = Object.keys(finalGames).length;
    if (totalGames === 0) {
      showNotification('No games found. Please check the website.', 'error');
      setButtonsDuringScan(false);
      retryBtn.style.display = 'inline-block';
      return;
    }
    if (isCancelled) {
      setButtonsDuringScan(false);
      return;
    }
    
    // Stop animation and set to full
    if (progressBar) {
      progressBar.classList.remove('indeterminate');
      progressBar.style.setProperty('--progress-width', '100%');
    }
    // Polished progress text: Combined setting for consistency
    const finalMessage = `Found ${totalGames} games. Displaying results...`;
    if (progressText) progressText.textContent = finalMessage;
    if (statusDiv) statusDiv.textContent = finalMessage;
    
    gamesData = finalGames;
    displayResults();
    if (gameCountDiv) {
      gameCountDiv.textContent = `Found ${totalGames} games`;
      gameCountDiv.style.display = 'block';
    }
    try {
      localStorage.setItem('gamesData', JSON.stringify(gamesData));
    } catch (storageError) {
      console.warn('Failed to save to localStorage:', storageError);
    }
    
    const duration = Math.round((Date.now() - startTime) / 1000);
    showDesktopNotification('Scan Completed', `Found ${totalGames} games in ${duration} seconds.`);
    
    // Auto-hide progress bar fast after complete
    setTimeout(() => {
      if (progressContainer) progressContainer.style.visibility = 'hidden';
      if (progressText) {
        progressText.style.visibility = 'hidden';
        progressText.textContent = '';
      }
    }, 500);
    
    setButtonsDuringScan(false);
  }

  function cancelScan() {
    isCancelled = true;
    setButtonsDuringScan(false);
    if (statusDiv) statusDiv.textContent = 'Scan cancelled.';
    showDesktopNotification('Scan Cancelled', 'The scraping process was cancelled.');
    if (progressContainer) {
      progressContainer.style.visibility = 'hidden';
    }
    if (progressText) {
      progressText.style.visibility = 'hidden';
      progressText.textContent = '';
    }
  }

  function clearCache() {
    if (window.confirm('Are you sure you want to clear the cache? This will remove all saved games and favorites.')) {
      localStorage.removeItem('gamesData');
      localStorage.removeItem('favorites');
      favorites = [];
      gamesData = {};
      resultsDiv.innerHTML = '';
      if (statusDiv) statusDiv.textContent = 'Cache cleared. Click "Start" to begin.';
      if (gameCountDiv) gameCountDiv.style.display = 'none';
      console.log('Cache cleared');
    }
  }

  function retryScraper() {
    retryBtn.style.display = 'none';
    runScraper();
  }

  function toggleSort() {
    sortByDate = !sortByDate;
    if (sortBtn) sortBtn.textContent = sortByDate ? 'Sort: Date' : 'Sort: Name';
    displayResults();
  }

  function toggleFavorites() {
    showFavoritesOnly = !showFavoritesOnly;
    displayResults();
    const total = Object.keys(showFavoritesOnly ? Object.fromEntries(Object.entries(gamesData).filter(([title]) => favorites.includes(title))) : gamesData).length;
    if (gameCountDiv) {
      gameCountDiv.textContent = `Found ${total} games`;
    }
    // Update favBtn color
    if (favBtn) {
      favBtn.style.color = showFavoritesOnly ? '#ffd700' : '#ccc';
    }
  }

  displayCachedResults();
  setButtonsDuringScan(false);

  // Ensure progress bar and text are always hidden on load
  if (progressContainer) progressContainer.style.visibility = 'hidden';
  if (progressText) {
    progressText.textContent = '';
    progressText.style.visibility = 'hidden';
  }

  discordLink.addEventListener('click', () => {
    shell.openExternal('https://discord.gg/nj45kDSBEd');
  });

  searchInput.addEventListener('input', () => {
    const query = searchInput.value.toLowerCase();
    const cards = document.querySelectorAll('.game-card');
    cards.forEach(card => {
      const title = card.querySelector('.game-title').textContent.toLowerCase();
      card.style.display = title.includes(query) ? 'block' : 'none';
    });
  });

  closeBtn.addEventListener('click', () => {
    modal.style.display = 'none';
  });

  fullImageClose.addEventListener('click', () => {
    fullImageModal.style.display = 'none';
  });

  window.addEventListener('click', (event) => {
    if (event.target === modal) {
      modal.style.display = 'none';
    }
    if (event.target === fullImageModal) {
      fullImageModal.style.display = 'none';
    }
  });

  // Add ESC key to close modal
  window.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && modal.style.display === 'block') {
      modal.style.display = 'none';
    }
    if (event.key === 'Escape' && fullImageModal.style.display === 'block') {
      fullImageModal.style.display = 'none';
    }
  });

  scrapeBtn.addEventListener('click', () => {
    console.log('Button clicked');
    runScraper();
  });
  if (cancelBtn) cancelBtn.addEventListener('click', cancelScan);
  if (clearCacheBtn) clearCacheBtn.addEventListener('click', clearCache);
  if (retryBtn) retryBtn.addEventListener('click', retryScraper);
  if (sortBtn) sortBtn.addEventListener('click', toggleSort);
  if (favBtn) favBtn.addEventListener('click', toggleFavorites);
});