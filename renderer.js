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
    if (scrapeBtn) scrapeBtn.disabled = scanning;
    if (cancelBtn) cancelBtn.disabled = !scanning;
    if (cancelBtn) cancelBtn.style.display = scanning ? 'inline-block' : 'none';
    if (clearCacheBtn) clearCacheBtn.disabled = scanning;
    if (retryBtn) retryBtn.disabled = scanning;
    if (sortBtn) sortBtn.disabled = scanning;
    if (favBtn) favBtn.disabled = scanning;
    if (searchInput) searchInput.disabled = scanning;
  }

  // Function to fetch games from RSS feed (supplemental data)
  async function getGamesFromRSS() {
    try {
      const rssUrl = `${BASE_URL}/category/ps4/feed/`;
      const response = await axios.get(rssUrl, { timeout: 3000 }); // Reduced timeout for speed
      const $ = cheerio.load(response.data, { xmlMode: true });
      const rssData = {};
      $('item').each((i, el) => {
        const title = $(el).find('title').text().text().trim();
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

  // Function to get all games from category pages (highly optimized for speed)
  async function getGamesFromCategory() {
    const games = {};
    let page = 1;
    const batchSize = 15; // Reverted to original for reliability
    while (true) {
      const promises = [];
      for (let i = 0; i < batchSize; i++) {
        const currentPage = page + i;
        const pageUrl = currentPage === 1 ? `${BASE_URL}/category/ps4/` : `${BASE_URL}/category/ps4/page/${currentPage}/`;
        promises.push(axios.get(pageUrl, { timeout: 5000 }).then(response => ({ page: currentPage, response })).catch(() => null)); // Reverted timeout
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
              games[cleanTitle] = { akira: [], viking: [], mediafire: [], onefichier: [], cover: cover.startsWith('http') ? cover : `${BASE_URL}${cover}`, voice: '', subtitles: '', notes: '', size: '', date: formattedDate, url: fullUrl, description: '', screenshots: [] };
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
      await new Promise(resolve => setTimeout(resolve, 25)); // Reverted delay
    }
    console.log('Games from category:', Object.keys(games).length);
    return games;
  }

  // Function to scrape a game page for Akira and Viking download links, description, and screenshots (lazy load)
  async function scrapeGamePage(gameUrl, gameTitle) {
    try {
      const response = await axios.get(gameUrl, { timeout: 3000 }); // Reduced timeout
      const $ = cheerio.load(response.data);
      // Collect download links by host
      const akiraLinks = [];
      const vikingLinks = [];
      const mediafireLinks = [];
      const onefichierLinks = [];
      $('a[href]').each((i, el) => {
        const href = $(el).attr('href');
        if (href) {
          const versionText = $(el).closest('p').text().trim().replace(/\s+/g, ' ') || $(el).closest('div').text().trim().replace(/\s+/g, ' ') || $(el).text().trim() || 'Download Link';
          // Extract version from link or text
          let version = href.match(/v(\d+\.\d+)/)?.[1] || versionText.match(/(\d+\.\d+)/)?.[1] || '';
          let type = versionText.toLowerCase().includes('update') ? 'update' : 'game';
          const linkData = { link: href, version: versionText, extractedVersion: version, type: type };
          if (href.includes('akirabox.com')) {
            akiraLinks.push(linkData);
          } else if (href.includes('vikingfile.com')) {
            vikingLinks.push(linkData);
          } else if (href.includes('mediafire.com')) {
            // Clean MediaFire link by removing trailing '/file'
            linkData.link = href.replace(/\/file$/, '');
            mediafireLinks.push(linkData);
          } else if (href.includes('1fichier.com')) {
            onefichierLinks.push(linkData);
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
        gamesData[gameTitle].onefichier = onefichierLinks;
        gamesData[gameTitle].cover = cover || gamesData[gameTitle].cover;
        gamesData[gameTitle].voice = voice;
        gamesData[gameTitle].subtitles = subtitles;
        gamesData[gameTitle].notes = notes;
        gamesData[gameTitle].size = size;
        gamesData[gameTitle].date = date || gamesData[gameTitle].date;
        gamesData[gameTitle].description = description;
        gamesData[gameTitle].screenshots = screenshots;
        gamesData[gameTitle].pageTitle = title;
        const pageCUSA = $('body').text().match(/CUSA\d+/) ? $('body').text().match(/CUSA\d+/)[0] : null;
        gamesData[gameTitle].cusa = pageCUSA;
        try {
          localStorage.setItem('gamesData', JSON.stringify(gamesData));
        } catch (storageError) {
          console.warn('Failed to save to localStorage:', storageError);
        }
      }
      
      return { akira: akiraLinks, viking: vikingLinks, mediafire: mediafireLinks, onefichier: onefichierLinks, title, metaDesc, cover, voice, subtitles, notes, size, date, description, screenshots };
    } catch (error) {
      console.error('Error scraping game page:', error);
      showNotification('Failed to load game details.', 'error');
      return { akira: [], viking: [], mediafire: [], onefichier: [], title: '', metaDesc: '', cover: '', voice: '', subtitles: '', notes: '', size: '', date: '', description: '', screenshots: [] };
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
    
    if (data.akira.length === 0 && data.viking.length === 0 && data.mediafire.length === 0 && data.onefichier.length === 0 && data.url) {
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
    
    // Helper function to create link sections with game/update separation
    function createLinkSection(title, links) {
      if (!links || links.length === 0) {
        const noLinks = document.createElement('p');
        noLinks.textContent = `No ${title} links available yet. Try refreshing the modal.`;
        return noLinks;
      }
      
      const section = document.createElement('div');
      section.className = 'links-section';
      section.innerHTML = `<h3 style="text-decoration: underline;">${title}:</h3>`;
      
      const gameLinks = links.filter(item => item.type === 'game');
      const updateLinks = links.filter(item => item.type === 'update');
      
      if (gameLinks.length > 0) {
        const gameSubSection = document.createElement('div');
        gameSubSection.innerHTML = '<h4>Game:</h4>';
        const gameList = document.createElement('ul');
        gameList.className = 'link-list';
        gameLinks.forEach(item => {
          const li = document.createElement('li');
          const a = document.createElement('a');
          a.href = item.link;
          // Construct file name
          const codeMatch = item.version.match(/(CUSA\d+)/) || (data.cusa && data.cusa.match(/(CUSA\d+)/)) || (data.pageTitle && data.pageTitle.match(/(CUSA\d+)/)) || gameTitle.match(/(CUSA\d+)/);
          const code = codeMatch ? codeMatch[1] : 'CUSA00000';
          const regionMatch = item.version.match(/–\s*([A-Z]{3})/) || (data.pageTitle && data.pageTitle.match(/–\s*([A-Z]{3})/)) || gameTitle.match(/–\s*([A-Z]{3})/);
          const region = regionMatch ? regionMatch[1] : 'EUR';
          const gameName = gameTitle.split(' – ')[0].replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '');
          const versionStr = item.extractedVersion ? `V${item.extractedVersion.replace('.', '_')}` : 'V1_000';
          const typeStr = item.type.charAt(0).toUpperCase() + item.type.slice(1);
          const displayText = `${code}_–_${region}_${gameName}_${versionStr}_${typeStr}_By-[DLPSGAME.COM].zip`;
          console.log('code:', code, 'region:', region, 'gameName:', gameName, 'item.version:', item.version, 'data.cusa:', data.cusa, 'data.pageTitle:', data.pageTitle, 'gameTitle:', gameTitle);
          a.textContent = displayText;
          a.addEventListener('click', (e) => {
            e.preventDefault();
            shell.openExternal(item.link);
          });
          li.appendChild(a);
          gameList.appendChild(li);
        });
        gameSubSection.appendChild(gameList);
        section.appendChild(gameSubSection);
      }
      
      if (updateLinks.length > 0) {
        // Group update links by update info
        const updateGroups = {};
        updateLinks.forEach(item => {
          const updateInfo = item.version.split(':')[0].trim();
          if (!updateGroups[updateInfo]) updateGroups[updateInfo] = [];
          updateGroups[updateInfo].push(item);
        });
        
        for (const [updateInfo, groupLinks] of Object.entries(updateGroups)) {
          const updateSubSection = document.createElement('div');
          updateSubSection.innerHTML = `<h4>${updateInfo}:</h4>`;
          const updateList = document.createElement('ul');
          updateList.className = 'link-list';
          groupLinks.forEach(item => {
            const li = document.createElement('li');
            const a = document.createElement('a');
            a.href = item.link;
            // Construct file name
            const codeMatch = item.version.match(/(CUSA\d+)/) || (data.cusa && data.cusa.match(/(CUSA\d+)/)) || (data.pageTitle && data.pageTitle.match(/(CUSA\d+)/)) || gameTitle.match(/(CUSA\d+)/);
            const code = codeMatch ? codeMatch[1] : 'CUSA00000';
            const regionMatch = item.version.match(/–\s*([A-Z]{3})/) || (data.pageTitle && data.pageTitle.match(/–\s*([A-Z]{3})/)) || gameTitle.match(/–\s*([A-Z]{3})/);
            const region = regionMatch ? regionMatch[1] : 'EUR';
            const gameName = gameTitle.split(' – ')[0].replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '');
            const versionStr = item.extractedVersion ? `V${item.extractedVersion.replace('.', '_')}` : 'V1_000';
            const typeStr = item.type.charAt(0).toUpperCase() + item.type.slice(1);
            const displayText = `${code}_–_${region}_${gameName}_${versionStr}_${typeStr}_By-[DLPSGAME.COM].zip`;
            console.log('code:', code, 'region:', region, 'gameName:', gameName, 'item.version:', item.version, 'data.cusa:', data.cusa, 'data.pageTitle:', data.pageTitle, 'gameTitle:', gameTitle);
            a.textContent = displayText;
            a.addEventListener('click', (e) => {
              e.preventDefault();
              shell.openExternal(item.link);
            });
            li.appendChild(a);
            updateList.appendChild(li);
          });
          updateSubSection.appendChild(updateList);
          section.appendChild(updateSubSection);
        }
      }
      
      return section;
    }
    
    // Akira Links
    modalBody.appendChild(createLinkSection('Akira Links', data.akira));
    
    // Viking Links
    modalBody.appendChild(createLinkSection('Viking Links', data.viking));
    
    // MediaFire Links
    modalBody.appendChild(createLinkSection('MediaFire Links', data.mediafire));
    
    // OneFichier Links
    modalBody.appendChild(createLinkSection('1Fichier Links', data.onefichier));
    
    modal.style.display = 'block';
    modalBody.scrollTop = 0;
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
        finalGames[title] = { akira: [], viking: [], mediafire: [], onefichier: [], cover: data.cover, voice: '', subtitles: '', notes: '', size: '', date: data.date, url: data.url, description: '', screenshots: [] };
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

  // Ensure progress bar and text are hidden on load
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

  // Attach button listeners with debug logs
  if (scrapeBtn) {
    scrapeBtn.addEventListener('click', () => {
      console.log('Scrape button clicked');
      runScraper();
    });
  }
  if (cancelBtn) {
    cancelBtn.addEventListener('click', () => {
      console.log('Cancel button clicked');
      cancelScan();
    });
  }
  if (clearCacheBtn) {
    clearCacheBtn.addEventListener('click', () => {
      console.log('Clear Cache button clicked');
      clearCache();
    });
  }
  if (retryBtn) {
    retryBtn.addEventListener('click', () => {
      console.log('Retry button clicked');
      retryScraper();
    });
  }
  if (sortBtn) {
    sortBtn.addEventListener('click', () => {
      console.log('Sort button clicked');
      toggleSort();
    });
  }
  if (favBtn) {
    favBtn.addEventListener('click', () => {
      console.log('Fav button clicked');
      toggleFavorites();
    });
  }
});