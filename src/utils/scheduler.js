// src/utils/scheduler.js
const cron = require('node-cron');
const shopifyService = require('../services/shopify.service');
const logger = require('./logger');

class Scheduler {
  start() {
    const interval = process.env.SYNC_INTERVAL_MINUTES || 5;
    
    cron.schedule(`*/${interval} * * * *`, async () => {
      logger.info('=== Starting scheduled sync ===');
      
      try {
        await shopifyService.syncAll();  // Only Shopify
        logger.info('=== Scheduled sync completed ===');
      } catch (error) {
        logger.error('Scheduled sync failed', error);
      }
    });

    logger.info(`Scheduler started - running every ${interval} minutes`);
  }
}

module.exports = new Scheduler();
