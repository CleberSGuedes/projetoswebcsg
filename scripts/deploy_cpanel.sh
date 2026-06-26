#!/usr/bin/env bash
set -euo pipefail

BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"

case "$BRANCH" in
  main)
    APP_NAME="projetoswebcsg_app"
    DEPLOYPATH="/home/proj5954/projetoswebcsg_app"
    ;;
  homologacao)
    APP_NAME="projetoswebcsg_homolog"
    DEPLOYPATH="/home/proj5954/projetoswebcsg_homolog"
    ;;
  *)
    echo "Deploy bloqueado: branch '$BRANCH' nao possui ambiente configurado." >&2
    exit 1
    ;;
esac

/bin/mkdir -p "$DEPLOYPATH/public" "$DEPLOYPATH/tmp" "$DEPLOYPATH/logs"

/bin/rsync -a --delete \
  --exclude='.git' \
  --exclude='.env' \
  --exclude='passenger_wsgi.py' \
  --exclude='tmp' \
  --exclude='logs' \
  --exclude='public' \
  ./ "$DEPLOYPATH"

cat > "$DEPLOYPATH/public/.htaccess" <<HTACCESS
# DO NOT REMOVE. CLOUDLINUX PASSENGER CONFIGURATION BEGIN
PassengerAppRoot "$DEPLOYPATH"
PassengerBaseURI "/"
PassengerPython "/home/proj5954/virtualenv/$APP_NAME/3.11/bin/python"
# DO NOT REMOVE. CLOUDLINUX PASSENGER CONFIGURATION END
HTACCESS

printf 'ok\n' > "$DEPLOYPATH/public/ping.txt"

/bin/chmod 755 "$DEPLOYPATH"
/bin/chmod 755 "$DEPLOYPATH/public"
/bin/chmod 644 "$DEPLOYPATH/public/.htaccess" "$DEPLOYPATH/public/ping.txt"

/bin/touch "$DEPLOYPATH/tmp/restart.txt"

echo "Deploy concluido para branch $BRANCH em $DEPLOYPATH"