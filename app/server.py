import logging
import os

import uvicorn

logger = logging.getLogger(__name__)
APP_MODULE = "app.main:app"


def _port_from_env() -> int:
    for key in ("APP_PORT", "PORT"):
        value = os.getenv(key)
        if value:
            try:
                return int(value)
            except ValueError:
                logger.warning("Ignoring %s=%s (not an integer)", key, value)
    return 8000


def _ssl_kwargs() -> dict[str, str]:
    cert = os.getenv("SSL_CERT_FILE")
    key = os.getenv("SSL_KEY_FILE")
    if not cert and not key:
        return {}
    if not cert or not key:
        logger.warning("Both SSL_CERT_FILE and SSL_KEY_FILE are required for HTTPS, ignoring partial config.")
        return {}

    ssl_kwargs: dict[str, str] = {"ssl_certfile": cert, "ssl_keyfile": key}
    ca = os.getenv("SSL_CA_FILE")
    if ca:
        ssl_kwargs["ssl_ca_certs"] = ca
    password = os.getenv("SSL_KEY_PASSWORD")
    if password:
        ssl_kwargs["ssl_keyfile_password"] = password

    logger.info("Starting HTTPS server using %s/%s", cert, key)
    return ssl_kwargs


def main() -> None:
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = _port_from_env()
    log_level = os.getenv("UVICORN_LOG_LEVEL", "info")
    ssl_kwargs = _ssl_kwargs()
    uvicorn.run(
        APP_MODULE,
        host=host,
        port=port,
        log_level=log_level,
        **ssl_kwargs,
    )


if __name__ == "__main__":
    main()
