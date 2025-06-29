"""
Post-Processing Service for Video Processing Pipeline

This service implements the post-processing component of the Optifye video processing pipeline:
1. Receives inference results via HTTP API
2. Draws bounding boxes on detected objects
3. Uploads annotated frames to S3
4. Returns processing results

Data Flow:
Consumer → HTTP API → Post-Processing → S3 Upload → Response
"""

import os
import time
import logging
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from service import PostProcessingService
from models import PostProcessingRequest, PostProcessingResponse, ErrorResponse

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Optifye Post-Processing Service",
    description="Post-processing service for video inference results",
    version="1.0.0"
)

# Initialize post-processing service
s3_bucket = os.getenv('S3_BUCKET', 'optifye-processed-frames')
aws_region = os.getenv('AWS_REGION', 'ap-south-1')

post_processing_service = PostProcessingService(
    s3_bucket=s3_bucket,
    aws_region=aws_region
)

@app.get("/")
async def root():
    """
    Health check endpoint
    
    Returns:
        dict: Service status and configuration
    """
    stats = post_processing_service.get_statistics()
    return {
        "service": "Optifye Post-Processing Service",
        "status": "running",
        "s3_bucket": s3_bucket,
        "aws_region": aws_region,
        "statistics": stats if stats else "No Stats to show"  
    }

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    
    Returns:
        dict: Service health status
    """
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "s3_connected": post_processing_service.s3_client is not None
    }

@app.post("/process", response_model=PostProcessingResponse)
async def process_inference_result(request: PostProcessingRequest):
    """
    Process inference results and upload annotated frames to S3
    
    Args:
        request: PostProcessingRequest containing inference results
        
    Returns:
        PostProcessingResponse: Processing results with S3 URLs
        
    Raises:
        HTTPException: If processing fails
    """
    try:
        logger.info(f"Received processing request for batch {request.batch_id}")
        
        # Process inference result
        response = post_processing_service.process_inference_result(request)
        
        if response.status == "completed":
            logger.info(f"✅ Successfully processed batch {request.batch_id}")
            return response
        else:
            logger.error(f"❌ Failed to process batch {request.batch_id}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process batch {request.batch_id}"
            )
            
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/statistics")
async def get_statistics():
    """
    Get processing statistics
    
    Returns:
        dict: Processing statistics
    """
    stats = post_processing_service.get_statistics()
    return {
        "statistics": stats,
        "timestamp": time.time()
    }

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """
    Global exception handler
    
    Args:
        request: FastAPI request
        exc: Exception that occurred
        
    Returns:
        JSONResponse: Error response
    """
    logger.error(f"Unhandled exception: {exc}")
    error_response = ErrorResponse(
        error=str(exc),
        timestamp=time.time()
    )
    return JSONResponse(
        status_code=500,
        content=error_response.dict()
    )

def main():
    """
    Main entry point for development server
    
    This function:
    1. Starts the FastAPI development server
    2. Handles the entire service lifecycle
    """
    import uvicorn
    
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', '8003'))
    
    logger.info(f"Starting post-processing service on {host}:{port}")
    logger.info(f"S3 bucket: {s3_bucket}")
    logger.info(f"AWS region: {aws_region}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=True,
        log_level="info"
    )

if __name__ == "__main__":
    main()

"""
================================================================================
                                POST-PROCESSING SERVICE SUMMARY
================================================================================

This Post-Processing Service implements the final step of the Optifye video processing 
pipeline as an HTTP API service.

ARCHITECTURE:
Consumer → HTTP API → Post-Processing → S3 Upload → Response

KEY ENDPOINTS:

1. POST /process
   - Receives inference results from consumer
   - Processes frames and uploads to S3
   - Returns processing results with S3 URLs

2. GET /health
   - Health check endpoint
   - Returns service status and S3 connectivity

3. GET /statistics
   - Returns processing statistics
   - Tracks batches and frames processed

4. GET /
   - Root endpoint with service info
   - Shows configuration and statistics

DATA FLOW:
1. Consumer sends POST request with inference results
2. Service validates request using Pydantic models
3. Service processes frames (1-3 per batch)
4. Service uploads annotated frames to S3
5. Service returns response with S3 URLs

CONFIGURATION:
- S3_BUCKET: S3 bucket for processed frames
- AWS_REGION: AWS region (default: ap-south-1)
- HOST: Service host (default: 0.0.0.0)
- PORT: Service port (default: 8081)

ERROR HANDLING:
- Global exception handler for unhandled errors
- HTTP status codes for different error types
- Detailed error logging
- Graceful error responses

VALIDATION:
- Pydantic models for request/response validation
- Automatic data type conversion
- Required field validation
- Confidence score range validation

MONITORING:
- Health check endpoint
- Statistics endpoint
- Detailed logging
- Error tracking

DEPLOYMENT:
- FastAPI for high performance
- Uvicorn ASGI server
- Environment variable configuration
- Docker containerization ready


================================================================================
"""
