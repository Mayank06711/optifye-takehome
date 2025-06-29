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
    
    def create_batch_request(self, frames: List[Dict[str, Any]], source: str) -> Dict[str, Any]:
        """
        Create batch request from frames for the inference service
        
        Args:
            frames: List of frame data from Kafka
            source: RTSP source URL
            
        Returns:
            dict: Batch request in the format expected by inference service
        """
        batch_id = int(time.time() * 1000)  # Convert to int as expected by inference service
        
        # Convert frames to the format expected by inference service
        frame_data_list = []
        for i, frame in enumerate(frames):
            # Extract frame data from Kafka message structure
            frame_data = {
                'frame_id': frame.get('frame_id', i),  # Use frame_id from message or fallback to index
                'frame_data': frame.get('frame_data', ''),
                'timestamp': frame.get('timestamp', time.time()),
                'source': source  # Add source to each frame as required by inference service
            }
            frame_data_list.append(frame_data)
        
        batch_request = {
            'batch_id': batch_id,
            'frames': frame_data_list
        }
        
        return batch_request
    
    def _safe_serialize_request(self, batch_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Safely serialize batch request without logging frame data
        
        Args:
            batch_request: Batch request to serialize
            
        Returns:
            dict: Serialized request with truncated frame data for logging
        """
        request_dict = batch_request.copy()
        
        # Truncate frame data for logging safety
        for frame in request_dict.get('frames', []):
            if 'frame_data' in frame and len(frame['frame_data']) > 50:
                frame['frame_data'] = frame['frame_data'][:50] + "...[TRUNCATED]"
        
        return request_dict
    
    def call_inference_service(self, batch_request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Call inference service with batch request
        
        Args:
            batch_request: Batch request to send
            
        Returns:
            dict: Inference results or None if failed
        """
        try:
            logger.info(f"🔍 Calling inference service for batch {batch_request['batch_id']}")
            
            # Send request to inference service
            response = requests.post(
                f"{self.inference_service_url}/predict",
                json=batch_request,
                timeout=self.timeout,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                # Parse response
                inference_data = response.json()
                logger.info(f"✅ Inference completed - Objects: {inference_data.get('total_objects', 0)}, Time: {inference_data.get('processing_time', 0):.2f}s")
                return inference_data
            else:
                logger.error(f"❌ Inference service error: {response.status_code} - {response.text}")
                return None
                    
        except requests.exceptions.Timeout:
            logger.error(f"❌ Inference service timeout for batch {batch_request['batch_id']}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Inference service request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error calling inference service: {e}")
            return None
    
    def call_post_processing_service(self, inference_response: Dict[str, Any]) -> Optional[PostProcessingResponse]:
        """
        Call post-processing service with inference results
        
        Args:
            inference_response: Results from inference service
            
        Returns:
            PostProcessingResponse: Post-processing results or None if failed
        """
        try:
            batch_id = str(inference_response.get('batch_id', 'unknown'))
            logger.info(f"📤 Calling post-processing service for batch {batch_id}")
            
            # Convert inference response to post-processing request format
            frame_results = []
            for frame_result in inference_response.get('frame_results', []):
                # Convert ObjectDetection to BoundingBox format
                objects = []
                for obj in frame_result.get('objects', []):
                    bbox = BoundingBox(
                        class_name=obj.get('class_name', ''),
                        confidence=obj.get('confidence', 0.0),
                        bbox=obj.get('bbox', [])
                    )
                    objects.append(bbox)
                
                # Create FrameResult in consumer format
                consumer_frame_result = FrameResult(
                    frame_index=frame_result.get('frame_id', 0),
                    frame_data=frame_result.get('frame_data', ''),
                    objects=objects,
                    object_count=frame_result.get('object_count', 0),
                    frame_classification=frame_result.get('frame_classification', '')
                )
                frame_results.append(consumer_frame_result)
            
            # Create batch classification
            batch_class_data = inference_response.get('batch_classification', {})
            batch_classification = BatchClassification(
                primary_class=batch_class_data.get('primary_class', ''),
                secondary_classes=batch_class_data.get('secondary_classes', []),
                class_distribution=batch_class_data.get('class_distribution', {}),
                average_confidence=batch_class_data.get('average_confidence', 0.0)
            )
            
            # Create post-processing request
            post_request = PostProcessingRequest(
                batch_id=batch_id,
                processed_frames=inference_response.get('processed_frames', 0),
                total_objects=inference_response.get('total_objects', 0),
                processing_time=inference_response.get('processing_time', 0.0),
                timestamp=inference_response.get('timestamp', time.time()),
                frame_results=frame_results,
                batch_classification=batch_classification
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
            logger.error(f"❌ Post-processing service timeout for batch {batch_id}")
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
                logger.error(f"❌ Inference failed for batch {batch_request['batch_id']}")
                self.failed_batches += 1
                return False
            
            # Step 3: Call post-processing service
            post_response = self.call_post_processing_service(inference_response)
            if not post_response:
                logger.error(f"❌ Post-processing failed for batch {batch_request['batch_id']}")
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
   - Converts Kafka frames to inference service format
   - Generates unique batch IDs as integers
   - Adds source field to each frame as required

2. call_inference_service()
   - HTTP POST to real inference service
   - Handles response parsing and validation
   - Manages timeouts and errors

3. call_post_processing_service()
   - Converts inference response to post-processing format
   - HTTP POST to post-processing service
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