// src/services/shopify.service.js
const shopifyConnector = require('../connectors/shopify.connector');
const snowflakeConnector = require('../connectors/snowflake.connector');
const syncState = require('../models/state.model');
const { retry } = require('../utils/retry');
const logger = require('../utils/logger');
const { subDays, formatISO } = require('date-fns');

class ShopifyService {
  async syncCustomers() {
    try {
      logger.info('Starting Shopify customers sync');

      let updatedAtMin = syncState.getLastSync('shopify', 'customers');

      if (!updatedAtMin) {
        // First sync - get last 90 days
        updatedAtMin = formatISO(subDays(new Date(), 90));
      }

      // Fetch customers with retry
      const customers = await retry(async () => {
        return await shopifyConnector.fetchIncremental('customers', updatedAtMin);
      }, 3, 2000); // 3 retries, 2 second delay

      if (customers.length === 0) {
        logger.info('No new customers to sync');
        return;
      }

      // Map Shopify data to Snowflake schema
      const mappedData = customers.map(c => ({
        customer_id: c.id,
        first_name: c.first_name,
        last_name: c.last_name,
        email: c.email,
        phone: c.phone,
        created_at: c.created_at,
        updated_at: c.updated_at,
        address: c.default_address?.address1,
        city: c.default_address?.city,
        state: c.default_address?.province,
        zip_code: c.default_address?.zip,
        country: c.default_address?.country
      }));

      // Insert to Snowflake with retry
      await retry(async () => {
        await snowflakeConnector.insertBatch('CUSTOMERS', mappedData);
      }, 3, 2000);

      // Update state
      const latestUpdate = customers.reduce((max, c) =>
        new Date(c.updated_at) > new Date(max) ? c.updated_at : max,
        updatedAtMin
      );

      syncState.setLastSync('shopify', 'customers', latestUpdate);

      logger.info(`Synced ${customers.length} customers successfully`);
    } catch (error) {
      logger.error('Error syncing customers', error);
      throw error;
    }
  }

  async syncProducts() {
    try {
      logger.info('Starting Shopify products sync');

      let updatedAtMin = syncState.getLastSync('shopify', 'products');

      if (!updatedAtMin) {
        updatedAtMin = formatISO(subDays(new Date(), 90));
      }

      const products = await retry(async () => {
        return await shopifyConnector.fetchIncremental('products', updatedAtMin);
      }, 3, 2000);

      if (products.length === 0) {
        logger.info('No new products to sync');
        return;
      }

      const mappedData = products.map(p => ({
        product_id: p.id,
        product_name: p.title,
        description: p.body_html,
        category: p.product_type,
        price: p.variants[0]?.price,
        cost: p.variants[0]?.compare_at_price,
        sku: p.variants[0]?.sku,
        created_at: p.created_at,
        updated_at: p.updated_at,
        is_active: p.status === 'active',
        vendor: p.vendor
      }));

      await retry(async () => {
        await snowflakeConnector.insertBatch('PRODUCTS', mappedData);
      }, 3, 2000);

      const latestUpdate = products.reduce((max, p) =>
        new Date(p.updated_at) > new Date(max) ? p.updated_at : max,
        updatedAtMin
      );

      syncState.setLastSync('shopify', 'products', latestUpdate);

      logger.info(`Synced ${products.length} products successfully`);
    } catch (error) {
      logger.error('Error syncing products', error);
      throw error;
    }
  }

  async syncOrders() {
    try {
      logger.info('Starting Shopify orders sync');

      let updatedAtMin = syncState.getLastSync('shopify', 'orders');

      if (!updatedAtMin) {
        updatedAtMin = formatISO(subDays(new Date(), 90));
      }

      const orders = await retry(async () => {
        return await shopifyConnector.fetchIncremental('orders', updatedAtMin);
      }, 3, 2000);

      if (orders.length === 0) {
        logger.info('No new orders to sync');
        return;
      }

      // Map orders
      const mappedOrders = orders.map(o => ({
        order_id: o.id,
        customer_id: o.customer?.id,
        order_date: o.created_at,
        total_amount: o.total_price,
        status: o.financial_status,
        shipping_address: o.shipping_address?.address1,
        shipping_city: o.shipping_address?.city,
        shipping_state: o.shipping_address?.province,
        shipping_zip: o.shipping_address?.zip,
        shipping_country: o.shipping_address?.country,
        payment_method: o.payment_gateway_names[0],
        tax_amount: o.total_tax,
        shipping_amount: o.total_shipping_price_set?.shop_money?.amount,
        discount_amount: o.total_discounts,
        grand_total: o.total_price
      }));

      await retry(async () => {
        await snowflakeConnector.insertBatch('ORDERS', mappedOrders);
      }, 3, 2000);

      // Map order items
      const orderItems = [];
      orders.forEach(order => {
        order.line_items.forEach(item => {
          orderItems.push({
            order_item_id: item.id,
            order_id: order.id,
            product_id: item.product_id,
            quantity: item.quantity,
            unit_price: item.price,
            line_item_tax: item.tax_lines.reduce((sum, tax) => sum + parseFloat(tax.price), 0),
            line_item_shipping: 0,
            line_item_discount: item.total_discount,
            line_item_total: item.price * item.quantity
          });
        });
      });

      if (orderItems.length > 0) {
        await retry(async () => {
          await snowflakeConnector.insertBatch('ORDER_ITEMS', orderItems);
        }, 3, 2000);
      }

      const latestUpdate = orders.reduce((max, o) =>
        new Date(o.updated_at) > new Date(max) ? o.updated_at : max,
        updatedAtMin
      );

      syncState.setLastSync('shopify', 'orders', latestUpdate);

      logger.info(`Synced ${orders.length} orders and ${orderItems.length} order items`);
    } catch (error) {
      logger.error('Error syncing orders', error);
      throw error;
    }
  }

  async syncInventory() {
    try {
      logger.info('Starting Shopify inventory sync');

      let updatedAtMin = syncState.getLastSync('shopify', 'inventory');

      if (!updatedAtMin) {
        updatedAtMin = formatISO(subDays(new Date(), 90));
      }

      const inventoryItems = await retry(async () => {
        return await shopifyConnector.fetchIncremental('inventory_items', updatedAtMin);
      }, 3, 2000);

      if (inventoryItems.length === 0) {
        logger.info('No new inventory to sync');
        return;
      }

      const mappedData = inventoryItems.map(i => ({
        inventory_id: i.id,
        product_id: i.variant_id,
        quantity_available: i.available,
        reorder_level: null,
        last_restocked: i.updated_at,
        warehouse_location: null
      }));

      await retry(async () => {
        await snowflakeConnector.insertBatch('INVENTORY', mappedData);
      }, 3, 2000);

      const latestUpdate = inventoryItems.reduce((max, i) =>
        new Date(i.updated_at) > new Date(max) ? i.updated_at : max,
        updatedAtMin
      );

      syncState.setLastSync('shopify', 'inventory', latestUpdate);

      logger.info(`Synced ${inventoryItems.length} inventory items`);
    } catch (error) {
      logger.error('Error syncing inventory', error);
      throw error;
    }
  }

  async syncAll() {
    await snowflakeConnector.connect();

    try {
      await this.syncCustomers();
      await this.syncProducts();
      await this.syncOrders();
      await this.syncInventory();

      logger.info('All Shopify data synced successfully');
    } catch (error) {
      logger.error('Error in Shopify sync', error);
      throw error;
    } finally {
      await snowflakeConnector.close();
    }
  }
}

module.exports = new ShopifyService();