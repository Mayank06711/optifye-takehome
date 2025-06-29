"""
Input schemas for the inference service
Defines the structure of incoming requests
"""

from typing import List
from pydantic import BaseModel, Field, validator
import base64

class FrameData(BaseModel):
    """
    Individual frame data structure
    """
    timestamp: float = Field(..., description="When frame was captured")
    frame_id: int = Field(..., description="Sequential frame identifier")
    frame_data: str = Field(..., description="Base64 encoded frame")
    source: str = Field(..., description="Source RTSP stream URL")
    
    @validator('frame_data')
    def validate_base64(cls, v):
        """Validate that frame_data is valid base64"""
        try:
            base64.b64decode(v)
            return v
        except Exception:
            raise ValueError('frame_data must be valid base64 encoded string')
    
    @validator('source')
    def validate_source(cls, v):
        """Validate RTSP source URL"""
        if not v.startswith('rtsp://'):
            raise ValueError('source must be a valid RTSP URL')
        return v

class BatchRequest(BaseModel):
    """
    Batch request containing multiple frames
    """
    batch_id: int = Field(..., description="Unique batch identifier")
    frames: List[FrameData] = Field(..., description="List of frames to process")
    
    @validator('frames')
    def validate_frames(cls, v):
        """Validate that we have frames to process"""
        if not v:
            raise ValueError('frames list cannot be empty')
        if len(v) > 100:  # Limit batch size
            raise ValueError('batch size cannot exceed 100 frames')
        return v