"""Fleet file and the command-consumer boot order."""

import asyncio

from src.operator_app.app import Platform, boot_operator, platform
from src.operator_app.fleet_config import fleet_path, load_specs


def test_default_fleet_is_four_sim_assets():
    specs = load_specs(fleet_path())
    assert [spec.id for spec in specs] == ["uav-01", "uav-02", "ugv-01", "ugv-02"]
    assert {spec.adapter for spec in specs} == {"sim"}
    assert specs[0].home_lat == 17.3850
    assert specs[3].name == "Wolf-2"


def test_command_consumer_opens_after_the_fleet_is_registered(monkeypatch):
    order: list[str] = []

    async def start(self):
        order.append("start")
        assert self._commands_open is False

    async def register():
        order.append("register")
        platform.adapters["uav-01"] = object()

    async def open_command_consumer(self):
        order.append("commands")
        assert "uav-01" in platform.adapters
        self._commands_open = True

    monkeypatch.setattr(Platform, "start", start)
    monkeypatch.setattr("src.operator_app.app._register_configured_fleet", register)
    monkeypatch.setattr(Platform, "open_command_consumer", open_command_consumer)
    monkeypatch.setattr("src.operator_app.app.start_redis_listener", lambda: order.append("redis"))
    try:
        asyncio.run(boot_operator())
        assert order == ["start", "redis", "register", "commands"]
        assert platform._commands_open is True
    finally:
        platform.adapters.pop("uav-01", None)
        platform._commands_open = False
