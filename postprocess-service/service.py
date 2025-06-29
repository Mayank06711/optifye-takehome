"""
Post-Processing Service Layer

This module contains the business logic for:
- Processing inference results
- Drawing bounding boxes on frames
- Uploading annotated frames to S3
- Generating final results
"""

import cv2
import time
import logging
import boto3
import base64
import numpy as np
import os
import random
from typing import List, Dict, Any, Optional
from models import (
    PostProcessingRequest, PostProcessingResponse, ProcessedFrame,
    BatchStatistics, BoundingBox, FrameResult, BatchClassification
)

logger = logging.getLogger(__name__)

class PostProcessingService:
    """
    Service layer for post-processing operations
    
    This class handles:
    - Processing inference results
    - Drawing bounding boxes on frames
    - Uploading frames to S3
    - Generating final results
    """
    
    def __init__(self, s3_bucket: str, aws_region: str = 'ap-south-1'):
        """
        Initialize post-processing service
        
        Args:
            s3_bucket: S3 bucket name for processed frames
            aws_region: AWS region (default: ap-south-1)
        """
        self.s3_bucket = s3_bucket
        self.aws_region = aws_region
        
        # Initialize S3 client
        try:
            self.s3_client = boto3.client('s3', region_name=self.aws_region)
            logger.info(f"Initialized S3 client for region: {self.aws_region}")
            
            # Check if bucket exists, create if it doesn't
            self._ensure_bucket_exists()
            
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            self.s3_client = None
        
        # Statistics
        self.processed_batches = 0
        self.uploaded_frames = 0
        
        logger.info(f"Initialized PostProcessingService")
        logger.info(f"S3 bucket: {self.s3_bucket}")
        logger.info(f"AWS region: {self.aws_region}")
    
    def _ensure_bucket_exists(self):
        """
        Check if S3 bucket exists, create it if it doesn't
        """
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            logger.info(f"S3 bucket '{self.s3_bucket}' exists")
        except self.s3_client.exceptions.NoSuchBucket:
            logger.info(f"S3 bucket '{self.s3_bucket}' does not exist, creating...")
            try:
                # Create bucket
                self.s3_client.create_bucket(
                    Bucket=self.s3_bucket,
                    CreateBucketConfiguration={'LocationConstraint': self.aws_region}
                )
                logger.info(f"✅ Successfully created S3 bucket '{self.s3_bucket}'")
            except Exception as e:
                logger.error(f"Failed to create S3 bucket '{self.s3_bucket}': {e}")
        except Exception as e:
            logger.error(f"Error checking S3 bucket '{self.s3_bucket}': {e}")
    
    def base64_to_frame(self, frame_data: str) -> Optional[np.ndarray]:
        """
        Convert base64 string back to frame
        
        Args:
            frame_data: Base64 encoded frame data
            
        Returns:
            np.ndarray: Decoded frame or None if failed
        """
        try:
            frame_bytes = base64.b64decode(frame_data)
            frame_array = np.frombuffer(frame_bytes, dtype=np.uint8)
            return cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
        except Exception as e:
            logger.error(f"Failed to decode base64 frame: {e}")
            return None
    
    def draw_bounding_boxes(self, frame: np.ndarray, objects: List[BoundingBox]) -> np.ndarray:
        """
        Draw bounding boxes on frame
        
        Args:
            frame: Input frame
            objects: List of detected objects with bbox, class, confidence
            
        Returns:
            np.ndarray: Annotated frame
        """
        annotated_frame = frame.copy()
        
        for obj in objects:
            bbox = obj.bbox
            class_name = obj.class_name
            confidence = obj.confidence
            
            if len(bbox) >= 4:
                # Draw bounding box
                cv2.rectangle(
                    annotated_frame,
                    (int(bbox[0]), int(bbox[1])),
                    (int(bbox[2]), int(bbox[3])),
                    (0, 255, 0),  # Green color
                    2
                )
                
                # Draw label
                label = f"{class_name}: {confidence:.2f}"
                label_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 2)[0]
                
                cv2.rectangle(
                    annotated_frame,
                    (int(bbox[0]), int(bbox[1]) - label_size[1] - 10),
                    (int(bbox[0]) + label_size[0], int(bbox[1])),
                    (0, 255, 0),
                    -1
                )
                
                cv2.putText(
                    annotated_frame,
                    label,
                    (int(bbox[0]), int(bbox[1]) - 5),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.5,
                    (0, 0, 0),
                    2
                )
        
        return annotated_frame
    
    def upload_to_s3(self, frame: np.ndarray, filename: str) -> Optional[str]:
        """
        Upload frame to S3
        
        Args:
            frame: Frame to upload
            filename: S3 filename
            
        Returns:
            str: S3 URL if successful, None otherwise
        """
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return None
            
        try:
            # Encode frame to JPEG with high quality
            encode_params = [cv2.IMWRITE_JPEG_QUALITY, 95]
            success, buffer = cv2.imencode('.jpg', frame, encode_params)
            
            if not success:
                logger.error("Failed to encode frame to JPEG")
                return None
            
            # Create S3 key with proper structure
            s3_key = f"processed_frames/{filename}"
            
            # Upload to S3 with proper metadata
            response = self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=buffer.tobytes(),
                ContentType='image/jpeg',
                Metadata={
                    'processed-by': 'optifye-post-processor',
                    'upload-timestamp': str(int(time.time())),
                    'frame-type': 'annotated'
                }
            )
            
            # Verify upload was successful
            if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
                s3_url = f"s3://{self.s3_bucket}/{s3_key}"
                logger.info(f"📤 Successfully uploaded {filename} to S3: {s3_url}")
                return s3_url
            else:
                logger.error(f"Failed to upload {filename} - HTTP Status: {response.get('ResponseMetadata', {}).get('HTTPStatusCode')}")
                return None
            
        except self.s3_client.exceptions.NoSuchBucket:
            logger.error(f"S3 bucket '{self.s3_bucket}' does not exist")
            return None
        except self.s3_client.exceptions.NoSuchKey:
            logger.error(f"S3 key '{s3_key}' does not exist")
            return None
        except Exception as e:
            logger.error(f"Failed to upload {filename} to S3: {e}")
            return None
    
    def process_inference_result(self, request: PostProcessingRequest) -> PostProcessingResponse:
        """
        Process inference result and create annotated frames
        
        Args:
            request: Post-processing request from consumer
            
        Returns:
            PostProcessingResponse: Processing results with S3 URLs
        """
        start_time = time.time()
        
        try:
            batch_id = request.batch_id
            frame_results = request.frame_results
            
            logger.info(f"Processing inference result for batch {batch_id}")
            logger.info(f"📊 Batch {batch_id} - Total Frames: {len(frame_results)}")
            
            processed_frames = []
            s3_urls = []
            
            # Assignment requirement: Process at least one frame per batch
            # Randomly select 1-3 frames to process
            frames_to_process = min(random.randint(1, 3), len(frame_results))
            selected_indices = random.sample(range(len(frame_results)), frames_to_process)
            
            logger.info(f"🎯 Processing {frames_to_process} frames from batch {batch_id}")
            
            # Process selected frames
            for idx in selected_indices:
                try:
                    frame_result = frame_results[idx]
                    frame_index = frame_result.frame_index
                    objects = frame_result.objects
                    frame_data = frame_result.frame_data
                    
                    logger.info(f"🖼️ Processing frame {frame_index} with {len(objects)} objects")
                    
                    if frame_data:
                        # Decode original frame
                        original_frame = self.base64_to_frame(frame_data)
                        
                        if original_frame is not None:
                            # Draw bounding boxes
                            annotated_frame = self.draw_bounding_boxes(original_frame, objects)
                            
                            # Upload to S3
                            timestamp = int(time.time() * 1000)
                            filename = f"batch_{batch_id}_frame_{frame_index}_{timestamp}.jpg"
                            s3_url = self.upload_to_s3(annotated_frame, filename)
                            
                            if s3_url:
                                s3_urls.append(s3_url)
                                processed_frame = ProcessedFrame(
                                    frame_index=frame_index,
                                    objects=objects,
                                    s3_url=s3_url,
                                    filename=filename,
                                    detection_count=len(objects),
                                    frame_classification=frame_result.frame_classification
                                )
                                processed_frames.append(processed_frame)
                                self.uploaded_frames += 1
                                logger.info(f"✅ Frame {frame_index} processed and uploaded to S3")
                            else:
                                logger.warning(f"⚠️ Failed to upload frame {frame_index} to S3, but frame was processed")
                        else:
                            logger.warning(f"⚠️ Failed to decode frame {frame_index}")
                    else:
                        logger.warning(f"⚠️ No frame data for frame {frame_index}")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing frame {idx}: {e}")
                    continue  # Continue with next frame
            
            # Create batch statistics
            batch_statistics = BatchStatistics(
                total_frames_in_batch=len(frame_results),
                frames_processed=len(processed_frames),
                total_objects_detected=request.total_objects,
                processing_time=request.processing_time,
                batch_classification=request.batch_classification
            )
            
            # Create response
            processing_time = time.time() - start_time
            response = PostProcessingResponse(
                batch_id=batch_id,
                status="completed" if len(processed_frames) > 0 else "failed",
                processed_frames=len(processed_frames),
                s3_urls=s3_urls,
                total_detections=sum(len(f.objects) for f in processed_frames),
                processing_time=processing_time,
                timestamp=time.time(),
                batch_statistics=batch_statistics
            )
            
            self.processed_batches += 1
            
            logger.info(f"✅ Batch {batch_id} processed - Annotated: {len(processed_frames)}, Detections: {response.total_detections}")
            return response
            
        except Exception as e:
            logger.error(f"Error processing inference result: {e}")
            # Return error response
            return PostProcessingResponse(
                batch_id=request.batch_id,
                status="failed",
                processed_frames=0,
                s3_urls=[],
                total_detections=0,
                processing_time=time.time() - start_time,
                timestamp=time.time(),
                batch_statistics=BatchStatistics(
                    total_frames_in_batch=len(request.frame_results),
                    frames_processed=0,
                    total_objects_detected=request.total_objects,
                    processing_time=request.processing_time,
                    batch_classification=request.batch_classification
                )
            )
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get processing statistics
        
        Returns:
            dict: Processing statistics
        """
        return {
            'processed_batches': self.processed_batches,
            'uploaded_frames': self.uploaded_frames
        }

"""
================================================================================
                                POST-PROCESSING SERVICE SUMMARY
================================================================================

This service layer implements the business logic for post-processing, handling
S3 upload and frame annotation.

ARCHITECTURE:
Inference Results → Frame Processing → S3 Upload → Final Results

KEY METHODS:

1. process_inference_result()
   - Main processing orchestration
   - Randomly selects 1-3 frames per batch (assignment requirement)
   - Calls frame processing and S3 upload
   - Returns validated PostProcessingResponse

2. draw_bounding_boxes()
   - Draws bounding boxes on frames
   - Adds class names and confidence scores
   - Uses green color for visibility

3. upload_to_s3()
   - Encodes frames to JPEG
   - Uploads to S3 with metadata
   - Handles various S3 errors
   - Returns S3 URLs

4. base64_to_frame()
   - Decodes base64 frame data
   - Converts to OpenCV format
   - Error handling for invalid data


ERROR HANDLING:
- S3 upload failures: Logged and tracked
- Frame decode errors: Logged and skipped
- Invalid data: Graceful handling
- Statistics tracking: Success/failure rates

S3 STORAGE:
- Bucket: Configurable via environment
- Path: processed_frames/
- Files: batch_{id}_frame_{index}_{timestamp}.jpg
- Metadata: Processing information

PERFORMANCE:
- JPEG compression: 95% quality
- Random frame selection: 1-3 frames per batch
- Error recovery: Continues on individual failures
- Statistics: Tracks batches and frames

USAGE:
service = PostProcessingService(s3_bucket="my-bucket")

response = service.process_inference_result(request)

================================================================================
""" 