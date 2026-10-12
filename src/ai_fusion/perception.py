"""
AI Perception Layer + Tracking integration
------------------------------------------
YOLO detector is active.
Detections are automatically fed into the SimpleTracker.
"""

from typing import List, Optional
from pathlib import Path

from src.data_services.event_store import event_store, EventType
from src.kernel.models import Detection, DetectionLabel

DetectionResult = Detection


class BaseDetector:
    def detect(self, asset_id: str, lat: float, lon: float,
               image_path: Optional[str] = None,
               mission_id: Optional[str] = None) -> List[DetectionResult]:
        raise NotImplementedError


class SimulatedDetector(BaseDetector):
    """Fallback detector."""

    def detect(self, asset_id: str, lat: float, lon: float,
               image_path: Optional[str] = None,
               mission_id: Optional[str] = None) -> List[DetectionResult]:

        det = DetectionResult(
            asset_id=asset_id,
            lat=lat + 0.0022,
            lon=lon + 0.0015,
            label=DetectionLabel.OBJECT_OF_INTEREST,
            confidence=0.86,
            mission_id=mission_id,
            raw={"source": "simulated_detector"}
        )
        return [det]


class YOLODetector(BaseDetector):
    """Real detector using Ultralytics YOLOv8."""

    def __init__(self, model_name: str = "yolov8n.pt", conf_threshold: float = 0.35):
        from ultralytics import YOLO
        self.model = YOLO(model_name)
        self.conf_threshold = conf_threshold
        print(f"[YOLODetector] Loaded model: {model_name}")

    def detect(self, asset_id: str, lat: float, lon: float,
               image_path: Optional[str] = None,
               mission_id: Optional[str] = None) -> List[DetectionResult]:

        if image_path is None or not Path(image_path).exists():
            print(f"[YOLODetector] Image not found: {image_path}")
            return []

        results = self.model(image_path, conf=self.conf_threshold, verbose=False)
        detections = []

        vehicle_classes = {"car", "truck", "bus", "motorcycle"}
        person_classes = {"person"}

        for r in results:
            boxes = r.boxes
            if boxes is None:
                continue

            for box in boxes:
                cls_id = int(box.cls[0])
                conf = float(box.conf[0])
                name = r.names[cls_id].lower()

                if name in vehicle_classes:
                    label = DetectionLabel.VEHICLE
                elif name in person_classes:
                    label = DetectionLabel.PERSON
                else:
                    label = DetectionLabel.OBJECT_OF_INTEREST

                det = DetectionResult(
                    asset_id=asset_id,
                    lat=lat + 0.0018,
                    lon=lon + 0.0012,
                    label=label,
                    confidence=round(conf, 3),
                    bbox=box.xyxy[0].tolist(),
                    image_path=image_path,
                    mission_id=mission_id,
                    raw={"yolo_class": name, "cls_id": cls_id}
                )
                detections.append(det)

        return detections


class PerceptionService:
    def __init__(self, detector: Optional[BaseDetector] = None):
        self.detector = detector or SimulatedDetector()

    def process_asset(self, asset_id: str, lat: float, lon: float,
                      mission_id: Optional[str] = None,
                      image_path: Optional[str] = None) -> List[DetectionResult]:

        results = self.detector.detect(
            asset_id=asset_id,
            lat=lat,
            lon=lon,
            image_path=image_path,
            mission_id=mission_id
        )

        # -------------------------------------------------
        # Feed detections into the Tracker
        # -------------------------------------------------
        if results:
            try:
                from src.ai_fusion.tracking import tracker
                from src.data.ingest import ingest_track
                tracks = tracker.update(results)
                for t in tracks:
                    try:
                        body = t.model_dump(mode="json")
                        # Association status stays off operator dumps. Fusion
                        # still records it; durable prefers this key over state.
                        body["status"] = t.status.value
                        if t.source_asset:
                            body["source_asset"] = t.source_asset
                        ingest_track(body, actor="fusion")
                    except Exception as e:
                        print(f"[Tracking] ingest failed: {e}")
                    if t.hits == 1:  # newly created track
                        event_store.add(
                            EventType.INFO,
                            f"New Track {t.track_id} created ({t.label})",
                            asset_id=asset_id,
                            data={"track_id": t.track_id, "label": t.label}
                        )
            except Exception as e:
                print(f"[Tracking] update failed: {e}")

        # Persist detections + emit events
        for det in results:
            try:
                from src.data.ingest import ingest_detection
                ingest_detection(det.model_dump(mode="json"), actor="fusion")
            except Exception as e:
                print(f"[Perception] ingest failed: {e}")

            event_store.add(
                EventType.DETECTION,
                f"DETECTION by {asset_id} → {det.label.value} "
                f"(conf={det.confidence:.2f}) at {det.lat:.5f}, {det.lon:.5f}",
                asset_id=asset_id,
                severity="warning",
                data={
                    "detection_id": det.detection_id,
                    "label": det.label.value,
                    "confidence": det.confidence,
                    "lat": det.lat,
                    "lon": det.lon,
                    "source": det.raw.get("yolo_class", det.raw.get("source", "unknown"))
                }
            )

        return results


# -------------------------------------------------
# ACTIVE DETECTOR = YOLO
# -------------------------------------------------
perception_service = PerceptionService(detector=YOLODetector(model_name="yolov8n.pt"))
