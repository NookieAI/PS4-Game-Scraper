const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('api', {
  store: {
    get: (key) => ipcRenderer.invoke('store:get', key),
    set: (key, value) => ipcRenderer.invoke('store:set', key, value),
    delete: (key) => ipcRenderer.invoke('store:delete', key),
    clear: () => ipcRenderer.invoke('store:clear'),
    getSettings: () => ipcRenderer.invoke('store:getSettings'),
    setSetting: (key, value) => ipcRenderer.invoke('store:setSetting', key, value),
    setSettings: (settings) => ipcRenderer.invoke('store:setSettings', settings),
  },
  openExternal: (url) => ipcRenderer.invoke('open:external', url),
  fetchGameList: (url, timeout) => ipcRenderer.invoke('fetch:gameList', url, timeout),
  fetchRSS: (url) => ipcRenderer.invoke('fetch:rss', url),
  scrapeGamePage: (url, title) => ipcRenderer.invoke('fetch:gamePage', url, title),
});