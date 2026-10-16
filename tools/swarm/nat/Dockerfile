FROM node:24-bookworm
WORKDIR /sdk
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 iptables coturn socat netcat-openbsd procps && rm -rf /var/lib/apt/lists/*
COPY package.json package-lock.json ./
COPY packages ./packages
RUN npm ci
COPY tools/swarm/package.json tools/swarm/package-lock.json ./tools/swarm/
RUN npm ci --prefix tools/swarm && cd tools/swarm && npx playwright install --with-deps chromium
COPY tools/swarm ./tools/swarm
ENTRYPOINT ["bash", "tools/swarm/nat/suite.sh"]
