"""
Consumer Service for Video Processing Pipeline

This service implements the consumer component of the Optifye video processing pipeline:
1. Consumes frames from Kafka in batches of 25
2. Calls inference service via HTTP API
3. Calls post-processing service via HTTP API
4. Handles error recovery and statistics

Data Flow:
Kafka Frames → Consumer → Inference Service → Post-Processing Service
"""

import os
import time
import logging
import json
from typing import List, Dict, Any
from kafka import KafkaConsumer
from dotenv import load_dotenv
from service import ConsumerService

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Consumer:
    """
    Kafka consumer that processes video frames in batches
    
    This class handles:
    - Consuming frames from Kafka topics
    - Batching frames (25 per batch)
    - Calling inference and post-processing services
    - Error handling and recovery
    """
    
    def __init__(self):
        """
        Initialize the consumer
        
        Configuration:
        - KAFKA_BROKERS: Kafka server addresses
        - KAFKA_TOPIC: Topic to consume frames from
        - INFERENCE_SERVICE_URL: URL of inference service
        - POST_PROCESSING_SERVICE_URL: URL of post-processing service
        - BATCH_SIZE: Number of frames per batch (default: 25)
        """
        # Load configuration from environment variables
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', '13.232.212.150:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'video-frames')
        self.inference_service_url = os.getenv('INFERENCE_SERVICE_URL', 'http://localhost:8080')
        self.post_processing_service_url = os.getenv('POST_PROCESSING_SERVICE_URL', 'http://localhost:8081')
        self.batch_size = int(os.getenv('BATCH_SIZE', '25'))
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_brokers.split(','),
            value_deserializer=self._safe_json_deserializer,
            auto_offset_reset='earliest',
            group_id='consumer-group'
        )
        
        # Initialize service layer
        self.service = ConsumerService(
            kafka_brokers=self.kafka_brokers,
            inference_service_url=self.inference_service_url,
            post_processing_service_url=self.post_processing_service_url,
            batch_size=self.batch_size
        )
        
        # Batch processing state
        self.current_batch = []
        self.current_source = None
        
        logger.info(f"Consumer initialized - Topic: {self.kafka_topic}, Batch size: {self.batch_size}")
        logger.info(f"Services - Inference: {self.inference_service_url}, Post-process: {self.post_processing_service_url}")
    
    def _safe_json_deserializer(self, m):
        """
        Custom deserializer to handle non-JSON messages gracefully
        
        Args:
            m: Kafka message
            
        Returns:
            dict: Deserialized message as a dictionary, or {'_skip': True} for non-JSON
        """
        try:
            return json.loads(m.decode('utf-8'))
        except json.JSONDecodeError:
            logger.warning(f"Skipping non-JSON message")
            return {'_skip': True}
    
    def process_frame(self, frame_data: Dict[str, Any]):
        """
        Process a single frame and add to current batch
        
        Args:
            frame_data: Frame data from Kafka
        """
        # Check if message should be skipped
        if isinstance(frame_data, dict) and frame_data.get('_skip'):
            return
        
        # Extract frame information
        frame_data_str = frame_data.get('frame_data', '')
        source = frame_data.get('source', 'unknown')
        timestamp = frame_data.get('timestamp', time.time())
        
        # Add frame to current batch
        frame = {
            'frame_data': frame_data_str,
            'source': source,
            'timestamp': timestamp
        }
        
        self.current_batch.append(frame)
        
        # Set source for batch (use first frame's source)
        if not self.current_source:
            self.current_source = source
        
        # Check if batch is complete
        if len(self.current_batch) >= self.batch_size:
            self.process_batch()
    
    def process_batch(self):
        """
        Process the current batch through the pipeline
        """
        if not self.current_batch:
            logger.warning("No frames in current batch to process")
            return
        
        logger.info(f"Processing batch: {len(self.current_batch)} frames from {self.current_source}")
        
        # Process batch through service layer
        success = self.service.process_batch(self.current_batch, self.current_source)
        
        if success:
            logger.info(f"✅ Batch processed successfully")
        else:
            logger.error(f"❌ Batch processing failed")
        
        # Reset batch state
        self.current_batch = []
        self.current_source = None
        
        # Log statistics periodically
        stats = self.service.get_statistics()
        if stats['processed_batches'] % 10 == 0:  # Log every 10 batches
            logger.info(f"📊 Stats - Batches: {stats['processed_batches']}, Failed: {stats['failed_batches']}, Success Rate: {stats['success_rate']:.1f}%")
    
    def run(self):
        """
        Main processing loop
        
        This function:
        1. Continuously reads frames from Kafka
        2. Batches frames (25 per batch)
        3. Calls inference service
        4. Calls post-processing service
        5. Handles errors gracefully
        """
        logger.info("Starting consumer...")
        
        try:
            for message in self.consumer:
                try:
                    # Process frame
                    self.process_frame(message.value)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            # Process any remaining frames in batch
            if self.current_batch:
                logger.info(f"Processing final batch with {len(self.current_batch)} frames")
                self.process_batch()
            
            # Close consumer
            self.consumer.close()
            
            # Log final statistics
            stats = self.service.get_statistics()
            logger.info(f"Consumer stopped. Final stats - Batches: {stats['processed_batches']}, Failed: {stats['failed_batches']}, Success Rate: {stats['success_rate']:.1f}%")

def main():
    """
    Main entry point that creates and runs the consumer
    
    This function:
    1. Creates a Consumer instance
    2. Starts the message processing loop
    3. Handles the entire consumer lifecycle
    """
    consumer = Consumer()
    consumer.run()

if __name__ == "__main__":
    main()

"""
================================================================================
                                CONSUMER SUMMARY
================================================================================

This Consumer implements the batching and orchestration component of the Optifye 
video processing pipeline.

ARCHITECTURE:
Kafka Frames → Consumer → Inference Service → Post-Processing Service

KEY FEATURES:

1. Frame Batching
   - Collects frames from Kafka until batch size (25) is reached
   - Maintains batch integrity and source tracking
   - Handles partial batches on shutdown

2. Service Orchestration
   - Calls inference service with batch requests
   - Forwards results to post-processing service
   - Validates responses using Pydantic models

3. Error Handling
   - Graceful handling of non-JSON messages
   - Service failure recovery
   - Statistics tracking for monitoring

4. Configuration
   - Environment variable based configuration
   - Configurable batch size and timeouts
   - Service URL configuration

DATA FLOW:
1. Kafka Message → Frame Processing → Batch Collection
2. Batch Complete → Inference Service Call → Response Validation
3. Inference Response → Post-Processing Call → Final Result
4. Statistics Update → Continue Processing

CONFIGURATION:
- KAFKA_BROKERS: Kafka server addresses
- KAFKA_TOPIC: Topic to consume frames from
- INFERENCE_SERVICE_URL: Inference service endpoint
- POST_PROCESSING_SERVICE_URL: Post-processing service endpoint
- BATCH_SIZE: Number of frames per batch (default: 25)

ERROR RECOVERY:
- Non-JSON messages: Logged and skipped
- Service failures: Logged and tracked
- Partial batches: Processed on shutdown
- Statistics: Success/failure rate tracking

MONITORING:
- Batch processing statistics
- Success/failure rates
- Frame processing counts
- Service response times


================================================================================
"""
