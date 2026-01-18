#!/usr/bin/env bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt update
apt install -y locales

# Ensure the desired locale is enabled before generation so update-locale succeeds.
LOCALE=en_US.UTF-8
if grep -Eq "^#\\s*${LOCALE} UTF-8" /etc/locale.gen; then
	sed -i "s/^#\\s*\\(${LOCALE} UTF-8\\)/\\1/" /etc/locale.gen
elif ! grep -Eq "^${LOCALE} UTF-8" /etc/locale.gen; then
	printf "%s UTF-8\n" "${LOCALE}" >> /etc/locale.gen
fi
locale-gen "${LOCALE}"
update-locale LANG="${LOCALE}"

apt install -y python3 python3-venv python3-pip python3-setuptools \
  network-manager avahi-daemon avahi-utils curl git unzip

systemctl enable NetworkManager
systemctl enable avahi-daemon

APP_DIR="/opt/gspro"
chown -R pi:pi "${APP_DIR}"
chmod +x "${APP_DIR}/start.sh"
chmod +x "${APP_DIR}/scripts/start_setup_network.sh"
chmod +x "${APP_DIR}/scripts/stop_setup_network.sh"
chmod +x "${APP_DIR}/scripts/publish_mdns.sh"

cp "${APP_DIR}/scripts/gspro.service" /etc/systemd/system/
systemctl enable gspro.service

ln -sf "${APP_DIR}/start.sh" /usr/local/bin/gspro-start
