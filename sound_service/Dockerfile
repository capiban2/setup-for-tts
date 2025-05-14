FROM node:22-alpine

COPY index.js trie.js js_config.js package.json healthcheck.js /

RUN npm install

CMD ["node", "index.js"]

