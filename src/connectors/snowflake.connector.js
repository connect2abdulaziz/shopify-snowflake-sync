// src/connectors/snowflake.connector.js
const snowflake = require('snowflake-sdk');
const config = require('../config/snowflake.config');
const logger = require('../utils/logger');

class SnowflakeConnector {
  constructor() {
    this.connection = null;
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.connection = snowflake.createConnection(config);
      
      this.connection.connect((err, conn) => {
        if (err) {
          logger.error('Failed to connect to Snowflake', err);
          reject(err);
        } else {
          logger.info('Successfully connected to Snowflake');
          resolve(conn);
        }
      });
    });
  }

  async execute(query, binds = []) {
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText: query,
        binds: binds,
        complete: (err, stmt, rows) => {
          if (err) {
            logger.error('Query execution failed', { query, error: err });
            reject(err);
          } else {
            resolve(rows);
          }
        }
      });
    });
  }

  async insertBatch(tableName, data) {
    if (!data || data.length === 0) {
      logger.info(`No data to insert into ${tableName}`);
      return;
    }

    // Get column names from first record
    const columns = Object.keys(data[0]);
    const placeholders = columns.map(() => '?').join(', ');
    
    const query = `
      INSERT INTO ${tableName} 
      (${columns.join(', ')}) 
      VALUES (${placeholders})
    `;

    logger.info(`Inserting ${data.length} records into ${tableName}`);

    // Batch insert
    for (const record of data) {
      const values = columns.map(col => record[col]);
      await this.execute(query, values);
    }

    logger.info(`Successfully inserted ${data.length} records into ${tableName}`);
  }

  async close() {
    if (this.connection) {
      this.connection.destroy((err) => {
        if (err) {
          logger.error('Error closing Snowflake connection', err);
        } else {
          logger.info('Snowflake connection closed');
        }
      });
    }
  }
}

module.exports = new SnowflakeConnector();