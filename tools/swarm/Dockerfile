FROM node:24-bookworm
WORKDIR /sdk
COPY package.json package-lock.json ./
COPY packages ./packages
RUN npm ci
COPY tools/swarm/package.json tools/swarm/package-lock.json ./tools/swarm/
RUN npm ci --prefix tools/swarm && cd tools/swarm && npx playwright install --with-deps chromium --only-shell
COPY tools/swarm ./tools/swarm
ENTRYPOINT ["node", "tools/swarm/launch.mjs"]
