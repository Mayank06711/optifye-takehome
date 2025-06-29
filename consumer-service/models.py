"""
Request and Response Models for Consumer Service

This module defines Pydantic models for:
- Batch requests to inference service
- Inference responses from AI model
- Post-processing requests
- Error handling and validation
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

class FrameData(BaseModel):
    """Model for individual frame data"""
    frame_id: int = Field(..., description="Unique frame identifier")
    frame_data: str = Field(..., description="Base64 encoded frame data")
    timestamp: float = Field(..., description="Frame capture timestamp")
    
    def __repr__(self) -> str:
        """Custom repr to avoid logging base64 data"""
        return f"FrameData(frame_id={self.frame_id}, frame_data='[BASE64_DATA]', timestamp={self.timestamp})"
    
    def __str__(self) -> str:
        """Custom str to avoid logging base64 data"""
        return self.__repr__()

class BatchRequest(BaseModel):
    """Request model for sending batch to inference service"""
    batch_id: str = Field(..., description="Unique batch identifier")
    frames: List[FrameData] = Field(..., description="List of frames in batch")
    source: str = Field(..., description="RTSP source URL")
    timestamp: float = Field(default_factory=lambda: datetime.now().timestamp())

class BoundingBox(BaseModel):
    """Model for detected object bounding box"""
    class_name: str = Field(..., description="Detected object class")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Detection confidence")
    bbox: List[int] = Field(..., min_items=4, max_items=4, description="Bounding box coordinates [x1, y1, x2, y2]")

class FrameResult(BaseModel):
    """Model for inference result on a single frame"""
    frame_index: int = Field(..., description="Frame index in batch")
    frame_data: str = Field(..., description="Base64 encoded original frame")
    objects: List[BoundingBox] = Field(default=[], description="Detected objects")
    object_count: int = Field(default=0, description="Number of objects detected")
    frame_classification: str = Field(default="", description="Frame classification summary")
    
    def __repr__(self) -> str:
        """Custom repr to avoid logging base64 data"""
        return f"FrameResult(frame_index={self.frame_index}, frame_data='[BASE64_DATA]', objects={len(self.objects)}, object_count={self.object_count}, frame_classification='{self.frame_classification}')"
    
    def __str__(self) -> str:
        """Custom str to avoid logging base64 data"""
        return self.__repr__()

class BatchClassification(BaseModel):
    """Model for batch-level classification statistics"""
    primary_class: str = Field(..., description="Primary detected class")
    secondary_classes: List[str] = Field(default=[], description="Secondary detected classes")
    class_distribution: Dict[str, int] = Field(default={}, description="Class distribution counts")
    average_confidence: float = Field(..., ge=0.0, le=1.0, description="Average confidence across batch")

class InferenceResponse(BaseModel):
    """Response model from inference service"""
    batch_id: str = Field(..., description="Batch identifier")
    processed_frames: int = Field(..., description="Total frames processed")
    total_objects: int = Field(..., description="Total objects detected")
    processing_time: float = Field(..., description="Processing time in seconds")
    timestamp: float = Field(..., description="Processing timestamp")
    frame_results: List[FrameResult] = Field(..., description="Results for each frame")
    batch_classification: BatchClassification = Field(..., description="Batch classification statistics")

class PostProcessingRequest(BaseModel):
    """Request model for post-processing service"""
    batch_id: str = Field(..., description="Batch identifier")
    processed_frames: int = Field(..., description="Total frames processed")
    total_objects: int = Field(..., description="Total objects detected")
    processing_time: float = Field(..., description="Processing time in seconds")
    timestamp: float = Field(..., description="Processing timestamp")
    frame_results: List[FrameResult] = Field(..., description="Results for each frame")
    batch_classification: BatchClassification = Field(..., description="Batch classification statistics")

class PostProcessingResponse(BaseModel):
    """Response model from post-processing service"""
    batch_id: str = Field(..., description="Batch identifier")
    status: str = Field(..., description="Processing status")
    processed_frames: int = Field(..., description="Number of frames processed")
    s3_urls: List[str] = Field(default=[], description="S3 URLs of uploaded images")
    total_detections: int = Field(..., description="Total detections processed")
    processing_time: float = Field(..., description="Post-processing time")
    timestamp: float = Field(..., description="Completion timestamp")

class ErrorResponse(BaseModel):
    """Error response model"""
    error: str = Field(..., description="Error message")
    batch_id: Optional[str] = Field(None, description="Batch identifier if available")
    timestamp: float = Field(default_factory=lambda: datetime.now().timestamp())

"""
================================================================================
                                CONSUMER MODELS SUMMARY
================================================================================

This module defines Pydantic models for the consumer service to ensure type safety
and data validation across the entire pipeline.

KEY MODELS:

1. BatchRequest → Sent to inference service
   - Contains batch_id, frames list, source URL
   - Validates frame data structure

2. InferenceResponse → Received from inference service  
   - Contains complete inference results
   - Includes bounding boxes, classifications, statistics

3. PostProcessingRequest → Sent to post-processing service
   - Forwards inference results for S3 upload
   - Maintains data integrity

4. PostProcessingResponse → Received from post-processing service
   - Confirms S3 upload completion
   - Provides S3 URLs for access

VALIDATION FEATURES:
- Confidence scores: 0.0 to 1.0
- Bounding boxes: Exactly 4 coordinates
- Timestamps: Automatic generation
- Required fields: Enforced validation

ERROR HANDLING:
- ErrorResponse model for consistent error format
- Optional batch_id for error tracking
- Timestamp for error timing

LOGGING SAFETY:
- Custom __repr__ and __str__ methods prevent base64 data logging
- Frame data is masked as [BASE64_DATA] in logs
- Maintains data integrity while protecting logs

USAGE:
from models import BatchRequest, InferenceResponse, PostProcessingRequest

# Create batch request
batch_request = BatchRequest(
    batch_id="batch_001",
    frames=[...],
    source="rtsp://example.com/stream"
)

# Validate inference response
response = InferenceResponse(**inference_data)

================================================================================
""" 