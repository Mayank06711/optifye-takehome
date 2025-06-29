"""
FastAPI application for video inference service
Main entry point for the application
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from services.inference import InferenceService
from schemas.requests import BatchRequest
from schemas.responses import (
    BatchResponse, 
    HealthResponse, 
    ModelInfoResponse
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global service instance
inference_service: InferenceService = None
startup_time: float = 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager
    Handles startup and shutdown
    """
    global inference_service, startup_time
    
    # Startup
    logger.info("Starting inference service...")
    startup_time = time.time()
    
    try:
        # Initialize inference service
        inference_service = InferenceService(
            model_name="yolov5nu",  # Use the model file you have
            confidence_threshold=0.5
        )
        logger.info("Inference service started successfully")
    except Exception as e:
        logger.error(f"Failed to start inference service: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down inference service...")

# Create FastAPI app
app = FastAPI(
    title="Video Inference Service",
    description="Object detection service for video frame batches",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint
    """
    global inference_service, startup_time
    
    is_healthy = inference_service.is_healthy() if inference_service else False
    uptime = time.time() - startup_time
    
    # Get detection mode
    detection_mode = inference_service.get_detection_mode() if inference_service else "unknown"
    
    return HealthResponse(
        status="healthy" if is_healthy else "unhealthy",
        model_loaded=inference_service.detector.model_loaded if inference_service else False,
        service="inference",
        uptime=uptime
    )

@app.get("/model-info", response_model=ModelInfoResponse)
async def model_info():
    """
    Get model information
    """
    global inference_service
    
    if not inference_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    info = inference_service.get_model_info()
    
    return ModelInfoResponse(
        model=info['model'],
        version=info['version'],
        classes=info['classes'],
        input_size=info['input_size'],
        confidence_threshold=info['confidence_threshold']
    )

@app.post("/predict", response_model=BatchResponse)
async def predict(batch_request: BatchRequest):
    """
    Process a batch of video frames
    
    Args:
        batch_request: Batch request with frames
        
    Returns:
        Batch response with detection results
    """
    global inference_service
    
    if not inference_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        # Process batch
        response = inference_service.process_batch(batch_request)
        return response
        
    except Exception as e:
        logger.error(f"Error processing batch {batch_request.batch_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

@app.get("/")
async def root():
    """
    Root endpoint with service information
    """
    return {
        "service": "Video Inference Service",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "model_info": "/model-info",
            "predict": "/predict",
            "docs": "/docs"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)