// src/config/shopify.config.js
require('dotenv').config();

module.exports = {
  shopName: process.env.SHOPIFY_SHOP_NAME,
  accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
  apiVersion: process.env.SHOPIFY_API_VERSION || '2024-01',
  shopUrl: `https://${process.env.SHOPIFY_SHOP_NAME}.myshopify.com`,
};