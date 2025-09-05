from dataclasses import dataclass


@dataclass
class VideoMetadata:
    duration: float
    video_id: str
    original_name: str
    fps: float
    uri: str

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            duration=float(data["duration"]),
            video_id=data["video_id"],
            original_name=data["original_name"],
            fps=float(data["fps"]),
            uri=data["uri"],
        )


@dataclass
class Interval:
    scene_id: str
    video_id: str
    start: float
    end: float
