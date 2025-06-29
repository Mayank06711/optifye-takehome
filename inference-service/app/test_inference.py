"""
Test script for the inference service
Tests the /predict endpoint with a sample video
"""

import cv2
import base64
import requests
import json
import time
from typing import List, Dict, Any
import os

class InferenceTester:
    """
    Test class for the inference service
    """
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize tester
        
        Args:
            base_url: FastAPI service URL
        """
        self.base_url = base_url
        self.sample_video_path = "sample.mp4"  # Video file in same directory
    
    def extract_frames_from_video(self, video_path: str, max_frames: int = 25) -> List[Dict[str, Any]]:
        """
        Extract frames from video file
        
        Args:
            video_path: Path to video file
            max_frames: Maximum number of frames to extract
            
        Returns:
            List of frame data dictionaries
        """
        if not os.path.exists(video_path):
            print(f"Video file not found: {video_path}")
            return []
        
        print(f"Extracting frames from {video_path}...")
        
        # Open video file
        cap = cv2.VideoCapture(video_path)
        
        if not cap.isOpened():
            print(f"Error: Could not open video file {video_path}")
            return []
        
        frames = []
        frame_count = 0
        
        while frame_count < max_frames:
            ret, frame = cap.read()
            
            if not ret:
                break
            
            # Resize frame to reduce size (optional)
            frame = cv2.resize(frame, (640, 480))
            
            # Convert frame to base64
            _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
            frame_base64 = base64.b64encode(buffer).decode('utf-8')
            
            # Create frame data with RTSP URL (as required by schema)
            frame_data = {
                "timestamp": time.time() + frame_count * 0.033,  # 30 FPS
                "frame_id": frame_count,
                "frame_data": frame_base64,
                "source": "rtsp://localhost:8554/sample"  # Use RTSP URL format
            }
            
            frames.append(frame_data)
            frame_count += 1
        
        cap.release()
        print(f"Extracted {len(frames)} frames")
        return frames
    
    def create_test_batch(self, frames: List[Dict[str, Any]], batch_id: int = 1) -> Dict[str, Any]:
        """
        Create test batch request
        
        Args:
            frames: List of frame data
            batch_id: Batch identifier
            
        Returns:
            Batch request dictionary
        """
        return {
            "batch_id": batch_id,
            "frames": frames
        }
    
    def test_health_endpoint(self) -> bool:
        """
        Test health endpoint
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            response = requests.get(f"{self.base_url}/health", timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Health check passed: {data}")
                return True
            else:
                print(f"❌ Health check failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Health check error: {e}")
            return False
    
    def test_model_info(self) -> bool:
        """
        Test model info endpoint
        
        Returns:
            True if successful, False otherwise
        """
        try:
            response = requests.get(f"{self.base_url}/model-info", timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Model info: {data}")
                return True
            else:
                print(f"❌ Model info failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Model info error: {e}")
            return False
    
    def test_predict_endpoint(self, batch_request: Dict[str, Any]) -> bool:
        """
        Test predict endpoint
        
        Args:
            batch_request: Batch request data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"🚀 Sending batch {batch_request['batch_id']} with {len(batch_request['frames'])} frames...")
            
            start_time = time.time()
            response = requests.post(
                f"{self.base_url}/predict",
                json=batch_request,
                timeout=120  # 2 minutes timeout for processing
            )
            request_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Prediction successful!")
                print(f"   Processing time: {data.get('processing_time', 0):.2f}s")
                print(f"   Request time: {request_time:.2f}s")
                print(f"   Processed frames: {data.get('processed_frames', 0)}")
                print(f"   Total objects: {data.get('total_objects', 0)}")
                print(f"   Batch classification: {data.get('batch_classification', {}).get('primary_class', 'Unknown')}")
                
                # Print first few frame results
                frame_results = data.get('frame_results', [])
                if frame_results:
                    print(f"\n📊 Sample frame results:")
                    for i, frame_result in enumerate(frame_results[:3]):  # Show first 3 frames
                        objects = frame_result.get('objects', [])
                        print(f"   Frame {frame_result.get('frame_id', i)}: {len(objects)} objects - {frame_result.get('frame_classification', 'Unknown')}")
                        for obj in objects[:2]:  # Show first 2 objects per frame
                            print(f"     - {obj.get('class_name', 'Unknown')} ({obj.get('confidence', 0):.2f})")
                
                return True
            else:
                print(f"❌ Prediction failed: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Prediction error: {e}")
            return False
    
    def run_full_test(self):
        """
        Run complete test suite
        """
        print(" Starting Inference Service Test Suite")
        print("=" * 50)
        
        # Test 1: Health check
        print("\n1. Testing health endpoint...")
        if not self.test_health_endpoint():
            print("❌ Health check failed. Make sure the service is running!")
            return
        
        # Test 2: Model info
        print("\n2. Testing model info...")
        if not self.test_model_info():
            print("❌ Model info failed!")
            return
        
        # Test 3: Extract frames from video
        print("\n3. Extracting frames from video...")
        frames = self.extract_frames_from_video(self.sample_video_path)
        
        if not frames:
            print("❌ No video file found. Please add sample.mp4 to the directory.")
            return
        
        # Test 4: Create batch request
        print("\n4. Creating batch request...")
        batch_request = self.create_test_batch(frames, batch_id=1)
        
        # Test 5: Predict endpoint
        print("\n5. Testing predict endpoint...")
        success = self.test_predict_endpoint(batch_request)
        
        if success:
            print("\n🎉 All tests passed!")
        else:
            print("\n❌ Some tests failed!")
        
        print("\n" + "=" * 50)

def main():
    """
    Main function to run tests
    """
    # Create tester instance
    tester = InferenceTester()
    
    # Run tests
    tester.run_full_test()

if __name__ == "__main__":
    main()