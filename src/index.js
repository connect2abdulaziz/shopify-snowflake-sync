// src/index.js
require('dotenv').config();
const scheduler = require('./utils/scheduler');
const shopifyService = require('./services/shopify.service');
const logger = require('./utils/logger');

async function runInitialSync() {
  logger.info('Running initial sync...');
  
  try {
    await shopifyService.syncAll();  
    logger.info('Initial sync completed');
  } catch (error) {
    logger.error('Initial sync failed', error);
    process.exit(1);
  }
}

async function main() {
  logger.info('Starting Shopify-Snowflake Sync Service');
  
  await runInitialSync();
  scheduler.start();
  
  logger.info('Service is running. Press Ctrl+C to stop.');
}

main().catch(error => {
  logger.error('Fatal error', error);
  process.exit(1);
});