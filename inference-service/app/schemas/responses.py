"""
Output schemas for the inference service
Defines the structure of response data
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, ConfigDict

class ObjectDetection(BaseModel):
    """
    Individual object detection result
    """
    class_name: str = Field(..., description="Detected object class")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Detection confidence (0-1)")
    bbox: List[int] = Field(..., min_items=4, max_items=4, description="Bounding box [x1, y1, x2, y2]")

class FrameResult(BaseModel):
    """
    Processing result for a single frame
    """
    frame_id: int = Field(..., description="Original frame ID")
    timestamp: float = Field(..., description="Original frame timestamp")
    frame_data: str = Field(..., description="Original base64 frame data")
    objects: List[ObjectDetection] = Field(default_factory=list, description="Detected objects")
    object_count: int = Field(..., ge=0, description="Number of objects detected")
    frame_classification: str = Field(..., description="Frame classification based on objects")
    processing_error: Optional[str] = Field(None, description="Error message if processing failed")

class BatchClassification(BaseModel):
    """
    Batch-level classification summary
    """
    primary_class: str = Field(..., description="Most common object class")
    secondary_classes: List[str] = Field(default_factory=list, description="Other detected classes")
    class_distribution: Dict[str, int] = Field(..., description="Count of each class")
    average_confidence: float = Field(..., ge=0.0, le=1.0, description="Average confidence across all detections")

class BatchResponse(BaseModel):
    """
    Complete batch processing response
    """
    model_config = ConfigDict(protected_namespaces=())  # Fix the warning
    
    batch_id: int = Field(..., description="Original batch ID")
    processed_frames: int = Field(..., ge=0, description="Number of frames successfully processed")
    total_objects: int = Field(..., ge=0, description="Total objects detected across all frames")
    processing_time: float = Field(..., ge=0.0, description="Total processing time in seconds")
    frame_results: List[FrameResult] = Field(..., description="Results for each frame")
    batch_classification: BatchClassification = Field(..., description="Batch-level classification")
    timestamp: float = Field(..., description="Response timestamp")

class HealthResponse(BaseModel):
    """
    Health check response
    """
    status: str = Field(..., description="Service status")
    model_loaded: bool = Field(..., description="Whether model is loaded")
    service: str = Field(..., description="Service name")
    uptime: float = Field(..., description="Service uptime in seconds")

class ModelInfoResponse(BaseModel):
    """
    Model information response
    """
    model: str = Field(..., description="Model name")
    version: str = Field(..., description="Model version")
    classes: int = Field(..., description="Number of classes")
    input_size: str = Field(..., description="Model input size")
    confidence_threshold: float = Field(..., description="Default confidence threshold")