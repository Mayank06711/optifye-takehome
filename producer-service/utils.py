#!/usr/bin/env python3
"""
Utility functions for the RTSP Producer

This module contains helper functions for:
- Frame processing and conversion
- RTSP connection management
- Image utilities
"""

import cv2
import time
import logging
import base64
import numpy as np
from typing import Optional

logger = logging.getLogger(__name__)

def resize_frame(frame: np.ndarray, max_width: int = 640, max_height: int = 480) -> np.ndarray:
    """
    Resize frame to reduce message size while maintaining aspect ratio
    
    Args:
        frame: OpenCV frame as numpy array
        max_width: Maximum width for resized frame
        max_height: Maximum height for resized frame
        
    Returns:
        np.ndarray: Resized frame
    """
    height, width = frame.shape[:2]
    
    # Calculate new dimensions maintaining aspect ratio
    if width > height:
        new_width = min(width, max_width)
        new_height = int(height * (new_width / width))
    else:
        new_height = min(height, max_height)
        new_width = int(width * (new_height / height))
    
    # Resize frame
    resized_frame = cv2.resize(frame, (new_width, new_height), interpolation=cv2.INTER_AREA)
    return resized_frame

def frame_to_base64(frame: np.ndarray, quality: int = 20) -> str:
    """
    Convert OpenCV frame (numpy array) to base64 string
    
    Why base64?
    - Kafka messages must be strings/bytes
    - Images are binary data (numpy arrays)
    - Base64 encoding converts binary to text
    - JSON can only contain text, not binary data
    
    Args:
        frame: OpenCV frame as numpy array (BGR format)
        quality: JPEG quality (1-100, default: 20 for much smaller messages)
        
    Returns:
        str: Base64 encoded JPEG image
    """
    # Resize frame first to reduce size
    resized_frame = resize_frame(frame)
    
    # Encode frame as JPEG with very low quality for smaller messages
    _, buffer = cv2.imencode('.jpg', resized_frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    # Convert to base64 string for JSON serialization
    return base64.b64encode(buffer).decode('utf-8')

def create_frame_message(frame: np.ndarray, timestamp: float, source: str, frame_id: int) -> dict:
    """
    Create a Kafka message structure for a single frame
    
    Args:
        frame: OpenCV frame as numpy array
        timestamp: Unix timestamp when frame was captured
        source: Source RTSP stream URL
        frame_id: Sequential frame identifier
        
    Returns:
        dict: Message structure with metadata and base64 frame
    """
    # Convert frame to base64
    frame_data = frame_to_base64(frame)
    
    # Create message structure with metadata
    return {
        'timestamp': timestamp,           # When frame was captured
        'frame_id': frame_id,             # Sequential frame identifier
        'frame_data': frame_data,         # Base64 encoded frame
        'source': source                  # Source RTSP stream
    }

def connect_rtsp_with_retry(rtsp_url: str, max_retries: int = 3, retry_delay: int = 5) -> Optional[cv2.VideoCapture]:
    """
    Connect to RTSP stream with retry mechanism
    
    Args:
        rtsp_url: RTSP stream URL
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        
    Returns:
        cv2.VideoCapture: OpenCV video capture object if successful, None otherwise
    """
    for attempt in range(max_retries):
        logger.info(f"Attempting to connect to RTSP stream (attempt {attempt + 1}/{max_retries}): {rtsp_url}")
        
        # Create video capture object
        cap = cv2.VideoCapture(rtsp_url)
        
        # Wait a bit for connection to establish
        time.sleep(2)
        
        # Check if connection was successful
        if cap.isOpened():
            logger.info(f"Successfully connected to RTSP stream: {rtsp_url}")
            return cap
        else:
            logger.warning(f"Failed to connect to RTSP stream (attempt {attempt + 1}/{max_retries})")
            cap.release()
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to RTSP stream after {max_retries} attempts")
                return None
    
    return None

def save_frame_as_image(frame: np.ndarray, filename: str = "captured_frame.jpg"):
    """
    Save a single frame as an image file
    
    Args:
        frame: OpenCV frame as numpy array
        filename: Output filename (default: captured_frame.jpg)
    """
    # Resize frame to a reasonable size for viewing
    resized_frame = resize_frame(frame, max_width=800, max_height=600)
    
    # Save the frame as JPEG
    success = cv2.imwrite(filename, resized_frame)
    if success:
        logger.info(f"✅ Frame saved as {filename}")
        logger.info(f"   Frame size: {resized_frame.shape[1]}x{resized_frame.shape[0]} pixels")
    else:
        logger.error(f"❌ Failed to save frame as {filename}")
    
    return success 