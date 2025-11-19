// src/services/backfill.service.js
const shopifyService = require('./shopify.service');
const facebookService = require('./facebook.service');
const syncState = require('../models/state.model');
const logger = require('../utils/logger');

class BackfillService {
  async backfillShopify(startDate, endDate, resources = ['customers', 'products', 'orders', 'inventory']) {
    logger.info(`Starting Shopify backfill from ${startDate} to ${endDate}`);
    
    // Temporarily override state
    const originalState = { ...syncState.state.shopify };
    
    resources.forEach(resource => {
      syncState.setLastSync('shopify', resource, startDate);
    });
    
    try {
      await shopifyService.syncAll();
      logger.info('Shopify backfill completed');
    } finally {
      // Restore original state
      syncState.state.shopify = originalState;
      syncState.save();
    }
  }

}

module.exports = new BackfillService();

// CLI usage:
// node src/services/backfill.service.js --start=2024-03-01 --end=2024-04-30