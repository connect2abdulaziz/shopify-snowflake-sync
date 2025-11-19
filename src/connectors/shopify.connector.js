// src/connectors/shopify.connector.js
const axios = require('axios');
const config = require('../config/shopify.config');
const logger = require('../utils/logger');

class ShopifyConnector {
  constructor() {
    this.baseUrl = `${config.shopUrl}/admin/api/${config.apiVersion}`;
    this.headers = {
      'X-Shopify-Access-Token': config.accessToken,
      'Content-Type': 'application/json'
    };
  }

  async fetchResource(resource, params = {}) {
    try {
      const url = `${this.baseUrl}/${resource}.json`;
      logger.debug(`Fetching Shopify resource: ${resource}`, { params });

      const response = await axios.get(url, {
        headers: this.headers,
        params: params
      });

      return response.data[resource];
    } catch (error) {
      logger.error(`Error fetching ${resource} from Shopify`, error);
      throw error;
    }
  }

  async fetchAllPages(resource, params = {}, limit = 250) {
    let allRecords = [];
    let hasMore = true;
    let pageInfo = null;

    params.limit = limit;

    while (hasMore) {
      const queryParams = { ...params };
      
      if (pageInfo) {
        queryParams.page_info = pageInfo;
      }

      const records = await this.fetchResource(resource, queryParams);
      allRecords = allRecords.concat(records);

      // Check for pagination (Link header in response)
      // Shopify uses cursor-based pagination
      hasMore = records.length === limit;
      
      if (hasMore && records.length > 0) {
        // Get last record ID for cursor
        pageInfo = records[records.length - 1].id;
        queryParams.since_id = pageInfo;
        delete queryParams.page_info;
      } else {
        hasMore = false;
      }

      logger.debug(`Fetched ${records.length} records, total: ${allRecords.length}`);
    }

    logger.info(`Total ${resource} fetched: ${allRecords.length}`);
    return allRecords;
  }

  async fetchIncremental(resource, updatedAtMin, params = {}) {
    params.updated_at_min = updatedAtMin;
    params.order = 'updated_at asc';
    
    return await this.fetchAllPages(resource, params);
  }
}

module.exports = new ShopifyConnector();