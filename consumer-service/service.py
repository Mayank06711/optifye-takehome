"""
Consumer Service Layer

This module contains the business logic for:
- Batching frames from Kafka
- Calling inference service via HTTP API
- Calling post-processing service via HTTP API
- Error handling and retry logic
"""

import time
import logging
import requests
from typing import List, Dict, Any, Optional
from kafka import KafkaConsumer
from models import (
    BatchRequest, InferenceResponse, PostProcessingRequest, 
    PostProcessingResponse, FrameData, ErrorResponse, FrameResult, BoundingBox, BatchClassification
)

logger = logging.getLogger(__name__)

class ConsumerService:
    """
    Service layer for consumer operations
    
    This class handles:
    - Frame batching from Kafka
    - HTTP calls to inference service
    - HTTP calls to post-processing service
    - Error handling and retries
    """
    
    def __init__(self, 
                 kafka_brokers: str,
                 inference_service_url: str,
                 post_processing_service_url: str,
                 batch_size: int = 25,
                 timeout: int = 30):
        """
        Initialize consumer service
        
        Args:
            kafka_brokers: Kafka server addresses
            inference_service_url: URL of inference service
            post_processing_service_url: URL of post-processing service
            batch_size: Number of frames per batch
            timeout: HTTP request timeout in seconds
        """
        self.kafka_brokers = kafka_brokers
        self.inference_service_url = inference_service_url
        self.post_processing_service_url = post_processing_service_url
        self.batch_size = batch_size
        self.timeout = timeout
        
        # Statistics
        self.processed_batches = 0
        self.failed_batches = 0
        self.total_frames_processed = 0
        
        logger.info(f"ConsumerService initialized - Batch size: {batch_size}, Timeout: {timeout}s")
    
    def create_batch_request(self, frames: List[Dict[str, Any]], source: str) -> BatchRequest:
        """
        Create batch request from frames
        
        Args:
            frames: List of frame data from Kafka
            source: RTSP source URL
            
        Returns:
            BatchRequest: Validated batch request
        """
        batch_id = f"batch_{int(time.time() * 1000)}"
        
        # Convert frames to FrameData models
        frame_data_list = []
        for i, frame in enumerate(frames):
            frame_data = FrameData(
                frame_id=i,
                frame_data=frame.get('frame_data', ''),
                timestamp=frame.get('timestamp', time.time())
            )
            frame_data_list.append(frame_data)
        
        batch_request = BatchRequest(
            batch_id=batch_id,
            frames=frame_data_list,
            source=source,
            timestamp=time.time()
        )
        
        return batch_request
    
    def _safe_serialize_request(self, batch_request: BatchRequest) -> Dict[str, Any]:
        """
        Safely serialize batch request without logging frame data
        
        Args:
            batch_request: Batch request to serialize
            
        Returns:
            dict: Serialized request with truncated frame data for logging
        """
        request_dict = batch_request.dict()
        
        # Truncate frame data for logging safety
        for frame in request_dict.get('frames', []):
            if 'frame_data' in frame and len(frame['frame_data']) > 50:
                frame['frame_data'] = frame['frame_data'][:50] + "...[TRUNCATED]"
        
        return request_dict
    
    def call_inference_service(self, batch_request: BatchRequest) -> Optional[InferenceResponse]:
        """
        Call inference service with batch request
        
        Args:
            batch_request: Batch request to send
            
        Returns:
            InferenceResponse: Inference results or None if failed
        """
        try:
            logger.info(f"🔍 Calling inference service for batch {batch_request.batch_id}")
            
            # For testing: Use dummy inference response
            # TODO: Replace with actual HTTP call when inference service is ready
            # inference_response = self.get_dummy_inference_response(batch_request)
            
            # REAL INFERENCE SERVICE CALL (uncomment when ready)
            try:
                # Send request to inference service
                response = requests.post(
                    f"{self.inference_service_url}/predict",
                    json=batch_request.dict(),
                    timeout=self.timeout,
                    headers={'Content-Type': 'application/json'}
                )
                
                if response.status_code == 200:
                    # Parse and validate response
                    inference_data = response.json()
                    inference_response = InferenceResponse(**inference_data)
                else:
                    logger.error(f"❌ Inference service error: {response.status_code} - {response.text}")
                    return None
                    
            except requests.exceptions.Timeout:
                logger.error(f"❌ Inference service timeout for batch {batch_request.batch_id}")
                return None
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Inference service request failed: {e}")
                return None
            
            logger.info(f"✅ Inference completed - Objects: {inference_response.total_objects}, Time: {inference_response.processing_time:.2f}s")
            
            return inference_response
                
        except Exception as e:
            logger.error(f"❌ Unexpected error calling inference service: {e}")
            return None
    
    def call_post_processing_service(self, inference_response: InferenceResponse) -> Optional[PostProcessingResponse]:
        """
        Call post-processing service with inference results
        
        Args:
            inference_response: Results from inference service
            
        Returns:
            PostProcessingResponse: Post-processing results or None if failed
        """
        try:
            logger.info(f"📤 Calling post-processing service for batch {inference_response.batch_id}")
            
            # Create post-processing request
            post_request = PostProcessingRequest(
                batch_id=inference_response.batch_id,
                processed_frames=inference_response.processed_frames,
                total_objects=inference_response.total_objects,
                processing_time=inference_response.processing_time,
                timestamp=inference_response.timestamp,
                frame_results=inference_response.frame_results,
                batch_classification=inference_response.batch_classification
            )
            
            # Send request to post-processing service
            response = requests.post(
                f"{self.post_processing_service_url}/process",
                json=post_request.dict(),
                timeout=self.timeout,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                # Parse and validate response
                post_data = response.json()
                post_response = PostProcessingResponse(**post_data)
                
                logger.info(f"✅ Post-processing completed - S3 URLs: {len(post_response.s3_urls)}, Time: {post_response.processing_time:.2f}s")
                
                return post_response
            else:
                logger.error(f"❌ Post-processing service error: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"❌ Post-processing service timeout for batch {inference_response.batch_id}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Post-processing service request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error calling post-processing service: {e}")
            return None
    
    def process_batch(self, frames: List[Dict[str, Any]], source: str) -> bool:
        """
        Process a batch of frames through the complete pipeline
        
        Args:
            frames: List of frame data from Kafka
            source: RTSP source URL
            
        Returns:
            bool: True if processing succeeded, False otherwise
        """
        try:
            # Step 1: Create batch request
            batch_request = self.create_batch_request(frames, source)
            
            # Step 2: Call inference service
            inference_response = self.call_inference_service(batch_request)
            if not inference_response:
                logger.error(f"❌ Inference failed for batch {batch_request.batch_id}")
                self.failed_batches += 1
                return False
            
            # Step 3: Call post-processing service
            post_response = self.call_post_processing_service(inference_response)
            if not post_response:
                logger.error(f"❌ Post-processing failed for batch {batch_request.batch_id}")
                self.failed_batches += 1
                return False
            
            # Step 4: Update statistics
            self.processed_batches += 1
            self.total_frames_processed += len(frames)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Unexpected error processing batch: {e}")
            self.failed_batches += 1
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get processing statistics
        
        Returns:
            dict: Processing statistics
        """
        return {
            'processed_batches': self.processed_batches,
            'failed_batches': self.failed_batches,
            'total_frames_processed': self.total_frames_processed,
            'success_rate': (self.processed_batches / (self.processed_batches + self.failed_batches)) * 100 if (self.processed_batches + self.failed_batches) > 0 else 0
        }

    def get_dummy_inference_response(self, batch_request: BatchRequest) -> InferenceResponse:
        """
        Generate dummy inference response for testing
        
        Args:
            batch_request: Batch request to simulate inference on
            
        Returns:
            InferenceResponse: Mock inference results
        """
        import random
        
        logger.info(f"🔬 Generating dummy inference response for batch {batch_request.batch_id}")
        
        # Simulate processing time
        processing_time = random.uniform(1.5, 3.0)
        
        # Generate frame results
        frame_results = []
        total_objects = 0
        
        for i, frame in enumerate(batch_request.frames):
            # Randomly decide if frame has detections
            has_detections = random.choice([True, True, False])  # 66% chance of detections
            
            if has_detections:
                # Generate random objects
                num_objects = random.randint(1, 3)
                objects = []
                
                for j in range(num_objects):
                    # Random object class
                    class_name = random.choice(['person', 'car', 'bicycle', 'dog', 'cat'])
                    
                    # Random confidence
                    confidence = random.uniform(0.6, 0.95)
                    
                    # Random bounding box (within frame bounds)
                    x1 = random.randint(50, 400)
                    y1 = random.randint(50, 300)
                    x2 = x1 + random.randint(50, 150)
                    y2 = y1 + random.randint(50, 150)
                    
                    bbox = BoundingBox(
                        class_name=class_name,
                        confidence=confidence,
                        bbox=[x1, y1, x2, y2]
                    )
                    objects.append(bbox)
                
                total_objects += len(objects)
                
                # Create frame result with truncated frame data
                frame_result = FrameResult(
                    frame_index=i,
                    frame_data="[TRUNCATED_FOR_LOGGING]",  # Don't log the actual frame data
                    objects=objects,
                    object_count=len(objects),
                    frame_classification=", ".join(set([obj.class_name for obj in objects]))
                )
            else:
                # Frame with no detections
                frame_result = FrameResult(
                    frame_index=i,
                    frame_data="[TRUNCATED_FOR_LOGGING]",  # Don't log the actual frame data
                    objects=[],
                    object_count=0,
                    frame_classification="Background"
                )
            
            frame_results.append(frame_result)
        
        # Generate batch classification
        all_classes = []
        for frame_result in frame_results:
            for obj in frame_result.objects:
                all_classes.append(obj.class_name)
        
        if all_classes:
            # Count class distribution
            class_counts = {}
            for class_name in all_classes:
                class_counts[class_name] = class_counts.get(class_name, 0) + 1
            
            # Find primary class
            primary_class = max(class_counts, key=class_counts.get)
            secondary_classes = [cls for cls in class_counts.keys() if cls != primary_class]
            
            # Calculate average confidence
            confidences = [obj.confidence for frame_result in frame_results for obj in frame_result.objects]
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
            
            batch_classification = BatchClassification(
                primary_class=primary_class,
                secondary_classes=secondary_classes,
                class_distribution=class_counts,
                average_confidence=avg_confidence
            )
        else:
            # No detections in batch
            batch_classification = BatchClassification(
                primary_class="Background",
                secondary_classes=[],
                class_distribution={},
                average_confidence=0.0
            )
        
        # Create inference response
        inference_response = InferenceResponse(
            batch_id=batch_request.batch_id,
            processed_frames=len(batch_request.frames),
            total_objects=total_objects,
            processing_time=processing_time,
            timestamp=time.time(),
            frame_results=frame_results,
            batch_classification=batch_classification
        )
        
        logger.info(f"🔬 Dummy inference completed - Objects: {total_objects}, Time: {processing_time:.2f}s")
        return inference_response

"""
================================================================================
                                CONSUMER SERVICE SUMMARY
================================================================================

This service layer implements the business logic for the consumer, handling the
complete pipeline from Kafka frames to final processing.

ARCHITECTURE:
Kafka Frames → Batch Request → Inference Service → Post-Processing Service

KEY METHODS:

1. create_batch_request()
   - Converts Kafka frames to validated BatchRequest
   - Generates unique batch IDs
   - Ensures data integrity

2. call_inference_service()
   - HTTP POST to inference service
   - Validates InferenceResponse
   - Handles timeouts and errors

3. call_post_processing_service()
   - HTTP POST to post-processing service
   - Forwards inference results
   - Validates PostProcessingResponse

4. process_batch()
   - Orchestrates complete pipeline
   - Handles errors and retries
   - Updates statistics

ERROR HANDLING:
- HTTP timeouts: Configurable timeout
- Service failures: Logged and tracked
- Validation errors: Pydantic validation
- Statistics tracking: Success/failure rates

CONFIGURATION:
- Batch size: Configurable (default: 25)
- Timeout: Configurable (default: 30s)
- Service URLs: Environment variables
- Kafka brokers: Environment variables

STATISTICS:
- Processed batches count
- Failed batches count
- Total frames processed
- Success rate calculation

USAGE:
service = ConsumerService(
    kafka_brokers="localhost:9092",
    inference_service_url="http://inference:8080",
    post_processing_service_url="http://postprocess:8080"
)

success = service.process_batch(frames, source)

================================================================================
""" 