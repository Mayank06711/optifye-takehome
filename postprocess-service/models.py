"""
Request and Response Models for Post-Processing Service

This module defines Pydantic models for:
- Post-processing requests from consumer
- Post-processing responses to consumer
- Error handling and validation
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

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

class BatchClassification(BaseModel):
    """Model for batch-level classification statistics"""
    primary_class: str = Field(..., description="Primary detected class")
    secondary_classes: List[str] = Field(default=[], description="Secondary detected classes")
    class_distribution: Dict[str, int] = Field(default={}, description="Class distribution counts")
    average_confidence: float = Field(..., ge=0.0, le=1.0, description="Average confidence across batch")

class PostProcessingRequest(BaseModel):
    """Request model for post-processing service"""
    batch_id: str = Field(..., description="Batch identifier")
    processed_frames: int = Field(..., description="Total frames processed")
    total_objects: int = Field(..., description="Total objects detected")
    processing_time: float = Field(..., description="Processing time in seconds")
    timestamp: float = Field(..., description="Processing timestamp")
    frame_results: List[FrameResult] = Field(..., description="Results for each frame")
    batch_classification: BatchClassification = Field(..., description="Batch classification statistics")

class ProcessedFrame(BaseModel):
    """Model for processed frame result"""
    frame_index: int = Field(..., description="Frame index in batch")
    objects: List[BoundingBox] = Field(..., description="Detected objects")
    s3_url: str = Field(..., description="S3 URL of uploaded image")
    filename: str = Field(..., description="S3 filename")
    detection_count: int = Field(..., description="Number of detections")
    frame_classification: str = Field(..., description="Frame classification")

class BatchStatistics(BaseModel):
    """Model for batch processing statistics"""
    total_frames_in_batch: int = Field(..., description="Total frames in original batch")
    frames_processed: int = Field(..., description="Number of frames actually processed")
    total_objects_detected: int = Field(..., description="Total objects detected in batch")
    processing_time: float = Field(..., description="Original processing time")
    batch_classification: BatchClassification = Field(..., description="Batch classification")

class PostProcessingResponse(BaseModel):
    """Response model from post-processing service"""
    batch_id: str = Field(..., description="Batch identifier")
    status: str = Field(..., description="Processing status")
    processed_frames: int = Field(..., description="Number of frames processed")
    s3_urls: List[str] = Field(default=[], description="S3 URLs of uploaded images")
    total_detections: int = Field(..., description="Total detections processed")
    processing_time: float = Field(..., description="Post-processing time")
    timestamp: float = Field(..., description="Completion timestamp")
    batch_statistics: BatchStatistics = Field(..., description="Batch statistics")

class ErrorResponse(BaseModel):
    """Error response model"""
    error: str = Field(..., description="Error message")
    batch_id: Optional[str] = Field(None, description="Batch identifier if available")
    timestamp: float = Field(default_factory=lambda: datetime.now().timestamp())

"""
================================================================================
                                POST-PROCESSING MODELS SUMMARY
================================================================================

This module defines Pydantic models for the post-processing service to ensure type 
safety and data validation for S3 upload and final result generation.

KEY MODELS:

1. PostProcessingRequest → Received from consumer
   - Contains complete inference results
   - Includes bounding boxes, classifications, statistics
   - Validates data structure before processing

2. PostProcessingResponse → Sent to consumer
   - Confirms S3 upload completion
   - Provides S3 URLs for access
   - Includes processing statistics

3. ProcessedFrame → Internal processing result
   - Tracks individual frame processing
   - Contains S3 URL and metadata
   - Used for final result compilation

4. BatchStatistics → Batch-level metrics
   - Tracks processing performance
   - Maintains classification data
   - Provides audit trail

VALIDATION FEATURES:
- Confidence scores: 0.0 to 1.0
- Bounding boxes: Exactly 4 coordinates
- Required fields: Enforced validation
- Data types: Automatic conversion

ERROR HANDLING:
- ErrorResponse model for consistent error format
- Optional batch_id for error tracking
- Timestamp for error timing

USAGE:
from models import PostProcessingRequest, PostProcessingResponse

# Validate incoming request
request = PostProcessingRequest(**incoming_data)

# Create response
response = PostProcessingResponse(
    batch_id=request.batch_id,
    status="completed",
    processed_frames=2,
    s3_urls=["s3://bucket/file1.jpg", "s3://bucket/file2.jpg"],
    total_detections=5,
    processing_time=1.23,
    timestamp=time.time()
)

================================================================================
""" 