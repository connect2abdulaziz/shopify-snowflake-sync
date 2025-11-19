// src/models/state.model.js
const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');

const STATE_FILE = path.join(__dirname, '../../state/sync-state.json');

class SyncState {
  constructor() {
    this.state = this.load();
  }

  load() {
    try {
      if (fs.existsSync(STATE_FILE)) {
        const data = fs.readFileSync(STATE_FILE, 'utf8');
        return JSON.parse(data);
      }
    } catch (error) {
      logger.error('Error loading state file', error);
    }
    
    // Default state
    return {
      shopify: {
        customers: { lastSync: null },
        products: { lastSync: null },
        orders: { lastSync: null },
        inventory: { lastSync: null }
      },
      facebook: {
        campaigns: { lastSync: null },
        adSets: { lastSync: null },
        ads: { lastSync: null },
        insights: { lastSync: null }
      }
    };
  }

  save() {
    try {
      const dir = path.dirname(STATE_FILE);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      
      fs.writeFileSync(STATE_FILE, JSON.stringify(this.state, null, 2));
      logger.debug('State saved successfully');
    } catch (error) {
      logger.error('Error saving state file', error);
    }
  }

  getLastSync(platform, resource) {
    return this.state[platform]?.[resource]?.lastSync;
  }

  setLastSync(platform, resource, timestamp) {
    if (!this.state[platform]) {
      this.state[platform] = {};
    }
    if (!this.state[platform][resource]) {
      this.state[platform][resource] = {};
    }
    
    this.state[platform][resource].lastSync = timestamp;
    this.save();
  }
}

module.exports = new SyncState();