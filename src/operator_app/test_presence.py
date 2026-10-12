"""Offline and lost each raise one warning, keyed by asset."""

import asyncio
from datetime import timedelta

from src.kernel.models import AlertLevel, Asset, AssetStatus, AssetType, Track, TrackState, TrackType
from src.operator_app.app import EntityStore, TrackManager, utcnow


class _Memory:
    async def save_asset(self, asset):
        return None

    async def save_track(self, track):
        return None

    async def save_alert(self, alert):
        return None

    async def save_mission(self, mission):
        return None


def test_offline_then_lost_is_one_warning_per_asset():
    async def body():
        store = EntityStore(_Memory())
        asset = Asset(
            id="ugv-02",
            name="Wolf-2",
            asset_type=AssetType.UGV,
            status=AssetStatus.OFFLINE,
            lat=17.38,
            lon=78.49,
        )
        store.assets[asset.id] = asset
        track = Track(
            id="own-ugv-02",
            label="Wolf-2",
            track_type=TrackType.FRIENDLY,
            state=TrackState.ACTIVE,
            lat=17.38,
            lon=78.49,
            source_asset_ids=["ugv-02"],
            last_update=utcnow() - timedelta(seconds=5),
        )
        store.tracks[track.id] = track
        manager = TrackManager(store)
        manager._asset_to_track[asset.id] = track.id
        await manager.lifecycle_tick()
        offline = [item for item in store.alerts.values() if item.id == "presence-ugv-02"]
        assert len(offline) == 1
        assert offline[0].level == AlertLevel.WARNING
        assert asset.status == AssetStatus.OFFLINE

        track.last_update = utcnow() - timedelta(seconds=30)
        await manager.lifecycle_tick()
        assert track.state == TrackState.LOST
        assert asset.status == AssetStatus.LOST
        warnings = [item for item in store.alerts.values() if item.id.startswith("presence-")]
        assert len(warnings) == 1
        assert "lost" in warnings[0].message.lower()
        assert warnings[0].level == AlertLevel.WARNING

        await manager.lifecycle_tick()
        assert len([item for item in store.alerts.values() if item.id.startswith("presence-")]) == 1

    asyncio.run(body())
