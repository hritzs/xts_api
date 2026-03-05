// ════════════════════════════════════════════════════════════════════════════
// GLOBAL VARIABLES
// ════════════════════════════════════════════════════════════════════════════

let ws = null;
let reconnectInterval = null;
let activeConfigBuild = null;
let activeMonitors = {};

// Price tracking for color changes
let previousPrices = {};
