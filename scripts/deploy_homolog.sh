#!/usr/bin/env bash
set -euo pipefail

DEPLOYPATH=/home/proj5954/projetoswebcsg_homolog

/bin/mkdir -p "$DEPLOYPATH/public" "$DEPLOYPATH/tmp" "$DEPLOYPATH/logs"

/bin/rsync -a --delete \
  --exclude='.git' \
  --exclude='.env' \
  --exclude='passenger_wsgi.py' \
  --exclude='tmp' \
  --exclude='logs' \
  --exclude='public' \
  ./ "$DEPLOYPATH"

cat > "$DEPLOYPATH/public/.htaccess" <<'HTACCESS'
# DO NOT REMOVE. CLOUDLINUX PASSENGER CONFIGURATION BEGIN
PassengerAppRoot "/home/proj5954/projetoswebcsg_homolog"
PassengerBaseURI "/"
PassengerPython "/home/proj5954/virtualenv/projetoswebcsg_homolog/3.11/bin/python"
# DO NOT REMOVE. CLOUDLINUX PASSENGER CONFIGURATION END
HTACCESS

printf 'ok\n' > "$DEPLOYPATH/public/ping.txt"

/bin/touch "$DEPLOYPATH/tmp/restart.txt"