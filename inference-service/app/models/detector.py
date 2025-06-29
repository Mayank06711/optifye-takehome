"""
YOLOv5 model wrapper for object detection
Handles model loading and inference
"""

import logging
import time
import base64
import os
from typing import List, Dict, Any, Optional
import numpy as np
import cv2
from ultralytics import YOLO

logger = logging.getLogger(__name__)

class ObjectDetector:
    """
    YOLOv5 object detection wrapper
    """
    
    def __init__(self, model_name: str = "yolov5nu", confidence_threshold: float = 0.5):
        """
        Initialize the object detector
        
        Args:
            model_name: YOLOv5 model variant (yolov5n, yolov5s, yolov5m, yolov5l, yolov5x)
            confidence_threshold: Minimum confidence for detections
        """
        self.model_name = model_name
        self.confidence_threshold = confidence_threshold
        self.model: Optional[YOLO] = None
        self.classes: List[str] = []
        self.model_loaded = False
        
        logger.info(f"Initializing ObjectDetector with model: {model_name}")
        self._load_model()
    
    def _load_model(self):
        """
        Load the YOLOv5 model
        """
        try:
            logger.info(f"Loading YOLOv5 model: {self.model_name}")
            
            # Try to load model directly
            model_path = f"{self.model_name}.pt"
            
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"Model file not found: {model_path}")
            
            # Load the model
            self.model = YOLO(model_path)
            
            # Get class names
            self.classes = list(self.model.names.values())
            
            self.model_loaded = True
            logger.info(f"Model loaded successfully. Classes: {len(self.classes)}")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            self.model_loaded = False
            raise RuntimeError(f"Model loading failed: {e}")
    
    def detect_objects(self, frame: np.ndarray) -> List[Dict[str, Any]]:
        """
        Detect objects in a single frame
        
        Args:
            frame: Input frame as numpy array (BGR format)
            
        Returns:
            List of detected objects with class, confidence, and bounding box
        """
        if not self.model_loaded or self.model is None:
            logger.error("Model not loaded")
            raise RuntimeError("YOLOv5 model is not loaded")
        
        try:
            # Run inference with YOLOv5
            results = self.model(frame, verbose=False)
            
            # Extract detections
            objects = []
            for detection in results[0].boxes:
                # Get detection data
                confidence = float(detection.conf[0].cpu().numpy())
                
                # Filter by confidence threshold
                if confidence < self.confidence_threshold:
                    continue
                
                # Get bounding box coordinates
                x1, y1, x2, y2 = detection.xyxy[0].cpu().numpy()
                
                # Get class information
                class_id = int(detection.cls[0].cpu().numpy())
                class_name = self.classes[class_id]
                
                objects.append({
                    'class_name': class_name,
                    'confidence': confidence,
                    'bbox': [int(x1), int(y1), int(x2), int(y2)]
                })
            
            return objects
            
        except Exception as e:
            logger.error(f"Error in YOLOv5 detection: {e}")
            raise RuntimeError(f"Object detection failed: {e}")
    
    def process_batch(self, frames: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a batch of frames
        
        Args:
            frames: List of frame data dictionaries with 'frame_data' (base64) key
            
        Returns:
            List of frame results with detections
        """
        results = []
        
        for i, frame_info in enumerate(frames):
            try:
                # Decode base64 frame
                frame_data = frame_info['frame_data']
                frame_bytes = base64.b64decode(frame_data)
                frame_array = np.frombuffer(frame_bytes, dtype=np.uint8)
                frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
                
                if frame is None:
                    logger.warning(f"Failed to decode frame {i}")
                    results.append({
                        'frame_id': frame_info.get('frame_id', i),
                        'timestamp': frame_info.get('timestamp', 0),
                        'frame_data': frame_data,
                        'objects': [],
                        'object_count': 0,
                        'frame_classification': 'Error',
                        'processing_error': 'Failed to decode frame'
                    })
                    continue
                
                # Detect objects using real YOLOv5
                objects = self.detect_objects(frame)
                
                # Classify frame based on objects
                frame_classification = self._classify_frame(objects)
                
                results.append({
                    'frame_id': frame_info.get('frame_id', i),
                    'timestamp': frame_info.get('timestamp', 0),
                    'frame_data': frame_data,
                    'objects': objects,
                    'object_count': len(objects),
                    'frame_classification': frame_classification,
                    'processing_error': None
                })
                
            except Exception as e:
                logger.error(f"Error processing frame {i}: {e}")
                results.append({
                    'frame_id': frame_info.get('frame_id', i),
                    'timestamp': frame_info.get('timestamp', 0),
                    'frame_data': frame_info.get('frame_data', ''),
                    'objects': [],
                    'object_count': 0,
                    'frame_classification': 'Error',
                    'processing_error': str(e)
                })
        
        return results
    
    def _classify_frame(self, objects: List[Dict[str, Any]]) -> str:
        """
        Classify frame based on detected objects
        
        Args:
            objects: List of detected objects
            
        Returns:
            Frame classification string
        """
        if not objects:
            return "Background"
        
        # Get unique classes
        classes = list(set([obj['class_name'] for obj in objects]))
        
        # Sort by frequency and confidence
        class_counts = {}
        for obj in objects:
            class_name = obj['class_name']
            class_counts[class_name] = class_counts.get(class_name, 0) + 1
        
        # Return primary classification
        if len(classes) == 1:
            return classes[0]
        else:
            primary_class = max(class_counts, key=class_counts.get)
            secondary_classes = [c for c in classes if c != primary_class]
            return f"{primary_class}, {', '.join(secondary_classes)}"
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        Get model information
        
        Returns:
            Dictionary with model details
        """
        return {
            'model': self.model_name,
            'version': '8.0.196',  # ultralytics version
            'classes': len(self.classes),
            'input_size': '640x640',  # YOLOv5 default
            'confidence_threshold': self.confidence_threshold,
            'model_loaded': self.model_loaded
        }