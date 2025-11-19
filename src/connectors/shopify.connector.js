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
    let url = `${this.baseUrl}/${resource}.json`;
    params.limit = limit;

    while (url) {
      try {
        const response = await axios.get(url, {
          headers: this.headers,
          params: params
        });

        const records = response.data[resource];
        allRecords = allRecords.concat(records);

        logger.debug(`Fetched ${records.length} records, total: ${allRecords.length}`);

        // Check for pagination in Link header
        const linkHeader = response.headers['link'];
        if (linkHeader && linkHeader.includes('rel="next"')) {
          // Extract next page URL from Link header
          const nextMatch = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
          if (nextMatch) {
            url = nextMatch[1];
            params = {}; // Clear params, URL already has them
          } else {
            url = null;
          }
        } else {
          url = null;
        }
      } catch (error) {
        logger.error(`Error fetching ${resource}`, error);
        throw error;
      }
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