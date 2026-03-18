/**
 * preload.js — context bridge between main and renderer
 * Exposes only safe, typed IPC calls to the renderer.
 */
const { contextBridge, ipcRenderer } = require('electron');

// Primary API
contextBridge.exposeInMainWorld('gameDB', {
  loadGames:     async ()    => await ipcRenderer.invoke('load-games-cache'),
  getDetails:    async (id)  => await ipcRenderer.invoke('scrape-game-details', id),
  refreshDetails: async (id) => await ipcRenderer.invoke('refresh-game-details', id),
  refreshAll:    async ()    => await ipcRenderer.invoke('check-new-games'),
  getDataSource: async ()    => await ipcRenderer.invoke('get-data-source'),
  openExternal:  async (url) => await ipcRenderer.invoke('open-external', url),
});

// Legacy alias — keeps backwards compatibility with existing index.html code
contextBridge.exposeInMainWorld('ps4Scraper', {
  loadPS4GamesCache:    async ()         => await ipcRenderer.invoke('load-games-cache'),
  getGameDetails:       async (id)       => await ipcRenderer.invoke('scrape-game-details', id),
  refreshGameDetails:   async (id)       => await ipcRenderer.invoke('refresh-game-details', id),
  checkNewGames:        async ()         => await ipcRenderer.invoke('check-new-games'),
  scrapePS4Games:       async ()         => await ipcRenderer.invoke('load-games-cache'),
  prependNewGames:      async ()         => await ipcRenderer.invoke('load-games-cache'),
  getProxyPort:         async ()         => await ipcRenderer.invoke('get-proxy-port'),
});

// Shell / window
contextBridge.exposeInMainWorld('electronShell', {
  openExternal: (url) => ipcRenderer.invoke('open-external', url),
});

// IPC event bus (for progress events etc.)
contextBridge.exposeInMainWorld('electronIPC', {
  on:                 (...args) => ipcRenderer.on(...args),
  removeAllListeners: (...args) => ipcRenderer.removeAllListeners(...args),
});
