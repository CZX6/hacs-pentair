"""Pentair coordinator."""

from __future__ import annotations

from datetime import timedelta
import json
import logging
from typing import Any
from urllib.parse import urljoin

from botocore.awsrequest import AWSRequest
from deepdiff import DeepDiff
from pypentair import Pentair
import requests

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DOMAIN

# Pentair Home write endpoint.  pypentair 0.4.1 exposes only read methods
# (`get_device`, `get_devices`), so we issue PUTs directly using the same
# SigV4-signed AWS Cognito auth the library already produces.  Body shape
# is `{"payload": {"<field>": "<value>"}}`; all values are strings.  Mirrors
# the pattern in thk-socal/hacs-pentair-switch's IntelliFlo 3 work.
_PENTAIR_BASE_URL = "https://api.pentair.cloud/"
_PENTAIR_DEVICE_PATH = "device/device-service/user/device/{device_id}"
_PENTAIR_USER_AGENT = "aws-amplify/4.3.10 react-native"

_LOGGER = logging.getLogger(__name__)
UPDATE_INTERVAL = 30


class PentairDataUpdateCoordinator(DataUpdateCoordinator):
    """Class to manage fetching data from the API."""

    def __init__(
        self, hass: HomeAssistant, config_entry: ConfigEntry, client: Pentair
    ) -> None:
        """Initialize."""
        self.api = client
        self.devices: dict[str, list[dict[str, Any]]] = {}
        self.device_coordinators: list[PentairDeviceDataUpdateCoordinator] = []

        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
        )

    def get_device(self, device_id: str) -> dict | None:
        """Get device by id."""
        return next(
            (
                device
                for device in self.devices.get("data", [])
                if device["deviceId"] == device_id
            ),
            None,
        )

    def get_devices(self, device_type: str | None = None) -> list[dict]:
        """Get device by id."""
        return [
            device
            for device in self.devices.get("data", [])
            if device_type is None or device["deviceType"] == device_type
        ]

    async def _async_update_data(self):
        """Update data via library, refresh token if necessary."""
        try:
            if devices := await self.hass.async_add_executor_job(self.api.get_devices):
                diff = DeepDiff(
                    self.devices,
                    devices,
                    ignore_order=True,
                    report_repetition=True,
                    verbose_level=2,
                )
                _LOGGER.debug("Devices updated: %s", diff if diff else "no changes")
                self.devices = devices
        except Exception as err:  # pylint: disable=broad-except
            _LOGGER.exception("Unknown exception while updating Pentair data: %s", err)
            raise UpdateFailed(err) from err
        return self.devices


class PentairDeviceDataUpdateCoordinator(DataUpdateCoordinator):
    """Class to manage fetching data from the device endpoint."""

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        client: Pentair,
        device_id: str,
    ) -> None:
        """Initialize."""
        self.api = client
        self.device_id = device_id

        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
        )

    def get_device_data(self) -> dict | None:
        """Get the device data."""
        if self.data and (data := self.data.get("data")):
            return data
        return None

    async def async_set_fields(self, fields: dict[str, str]) -> dict[str, Any]:
        """Send a SigV4-signed PUT updating one or more device fields.

        All field values must be strings (Pentair Home's contract — even
        numeric fields).  Returns the parsed response body.  Raises
        `RuntimeError` if Pentair doesn't reply with
        `code == "set_device_success"`.
        """
        return await self.hass.async_add_executor_job(
            _signed_put_sync, self.api, self.device_id, fields
        )

    async def _async_update_data(self):
        """Update data via library, refresh token if necessary."""
        try:
            if device := await self.hass.async_add_executor_job(
                self.api.get_device, self.device_id
            ):
                diff = DeepDiff(
                    self.data,
                    device,
                    ignore_order=True,
                    report_repetition=True,
                    verbose_level=2,
                )
                _LOGGER.debug(
                    "Device %s updated: %s",
                    self.device_id,
                    diff if diff else "no changes",
                )
                return device
        except Exception as err:  # pylint: disable=broad-except
            _LOGGER.exception("Unknown exception while updating Pentair data: %s", err)
            raise UpdateFailed(err) from err
        else:
            return None


def _signed_put_sync(
    client: Pentair, device_id: str, fields: dict[str, str]
) -> dict[str, Any]:
    """Blocking SigV4-signed PUT helper, called from `async_add_executor_job`.

    Build an AWSRequest, let pypentair's existing `get_auth()` add the SigV4
    signature, then `requests.request` with the signed headers.  Validate
    that Pentair acknowledged with `set_device_success`.
    """
    url = urljoin(_PENTAIR_BASE_URL, _PENTAIR_DEVICE_PATH.format(device_id=device_id))
    body = json.dumps({"payload": fields})
    req = AWSRequest(
        method="PUT",
        url=url,
        data=body,
        headers={
            "x-amz-id-token": client.id_token,
            "content-type": "application/json; charset=UTF-8",
            "user-agent": _PENTAIR_USER_AGENT,
        },
    )
    client.get_auth().add_auth(req)
    prepared = req.prepare()
    response = requests.request(
        method="PUT",
        url=prepared.url,
        data=body,
        headers=dict(prepared.headers),
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("data", {}).get("code") != "set_device_success":
        raise RuntimeError(f"Pentair API rejected write: {payload}")
    return payload
