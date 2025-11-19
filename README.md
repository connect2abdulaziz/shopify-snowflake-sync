# Project Structure
```
shopify-snowflake-sync/
├── src/
│   ├── config/
│   │   ├── shopify.config.js       ✓ Keep
│   │   └── snowflake.config.js     ✓ Keep
│   ├── connectors/
│   │   ├── shopify.connector.js    ✓ Keep
│   │   └── snowflake.connector.js  ✓ Keep
│   ├── services/
│   │   ├── shopify.service.js      ✓ Keep
│   │   └── sync.service.js         ✓ Keep (optional)
│   ├── models/
│   │   └── state.model.js          ✓ Keep
│   ├── utils/
│   │   ├── logger.js               ✓ Keep
│   │   ├── scheduler.js            ✓ Keep
│   │   └── retry.js                ✓ Keep (optional)
│   └── index.js                    ✓ Keep
├── .env
├── .env.example
├── package.json
└── README.md