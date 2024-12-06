import boto3
import io
import logging
from uuid import uuid4
from typing import Optional

from fastapi import APIRouter, File, UploadFile, HTTPException, StreamingResponse
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS S3 Configuration
AWS_ACCESS_KEY_1 = 'YOUR_ACCESS_KEY'  # Replace with your actual access key
AWS_SECRET_KEY_1 = 'YOUR_SECRET_KEY'  # Replace with your actual secret key
BUCKET_NAME_1 = 'YOUR_BUCKET_NAME'
REGION_1 = 'YOUR_REGION'  # Mumbai region

# Initialize S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_1,
    aws_secret_access_key=AWS_SECRET_KEY_1,
    region_name=REGION_1
)

# File Upload Functions
def upload_file_to_s3(file_bytes, file_extension):
    """
    Generic function to upload files to S3 with UUID-based naming
    """
    try:
        unique_name = f"{uuid4()}.{file_extension}"
        
        # Upload the file to S3 bucket
        in_memory_file = io.BytesIO(file_bytes)
        s3_client.upload_fileobj(in_memory_file, BUCKET_NAME_1, unique_name)
        
        # Return the unique name with extension
        return unique_name
    except Exception as e:
        logging.error(f"Failed to save file to S3: {str(e)}")
        raise Exception(f"Failed to save file to S3: {str(e)}")
    
def upload_pdf_to_s3(file: UploadFile):
    """
    Specific function to upload PDF files to S3
    """
    try:
        unique_name = f"{uuid4()}.pdf"
        file_content = file.file.read()
        s3_client.put_object(
            Bucket=BUCKET_NAME_1, 
            Key=unique_name, 
            Body=file_content, 
            ContentType='application/pdf'
        )
        logging.info(f"Uploaded PDF to S3 with key: {unique_name}")
        return unique_name
    except NoCredentialsError:
        logging.error("Credentials not available for AWS S3.")
        raise Exception("Credentials not available for AWS S3.")

# File Retrieval Functions
def get_pdf_from_s3(pdf_name: str):
    """
    Specific function to retrieve PDF files from S3
    """
    try:
        logging.info(f"Attempting to retrieve PDF from S3 with key: {pdf_name}")
        response = s3_client.get_object(Bucket=BUCKET_NAME_1, Key=pdf_name)
        pdf_content = response['Body'].read()
        logging.info(f"Successfully retrieved PDF from S3 with key: {pdf_name}")
        return pdf_content
    except s3_client.exceptions.NoSuchKey:
        logging.error(f"PDF not found in S3 bucket with key: {pdf_name}")
        raise HTTPException(status_code=404, detail="PDF not found in S3 bucket")
    except NoCredentialsError:
        logging.error("Credentials not available for AWS S3.")
        raise HTTPException(status_code=500, detail="Credentials not available for AWS S3.")
    
def get_file_from_s3(document_id: str) -> Optional[bytes]:
    """
    Generic function to get files from S3
    """
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME_1, Key=document_id)
        return response['Body'].read()
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == 'NoSuchKey':
            raise HTTPException(status_code=404, detail="File not found in S3")
        raise HTTPException(status_code=500, detail=f"S3 error: {str(e)}")

# FastAPI Router for File Upload and Download
router = APIRouter()

@router.post("/upload")
async def upload_file(file: UploadFile):
    """
    Upload file to S3 bucket with UUID-based naming
    Supports different file types (PDF, images, etc.)
    """
    try:
        # Get file extension
        file_extension = file.filename.split('.')[-1].lower()
        
        # Validate file extension
        allowed_extensions = ['pdf', 'jpg', 'jpeg', 'png', 'gif']
        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid file type. Allowed types: {', '.join(allowed_extensions)}"
            )
        
        # Choose upload method based on file extension
        if file_extension == 'pdf':
            # Use PDF-specific upload for PDFs
            unique_name = upload_pdf_to_s3(file)
        else:
            # Read file content for non-PDF files
            file_content = await file.read()
            # Use generic file upload for images
            unique_name = upload_file_to_s3(file_content, file_extension)
        
        # Return response with uploaded file details
        return {
            "status": "success",
            "message": "File uploaded successfully",
            "filename": unique_name,
            "download_url": f"/download/{unique_name}"
        }
    
    except Exception as e:
        # Handle any upload errors
        logging.error(f"Upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/download/{filename}")
async def download_file(filename: str):
    """
    Download file from S3 bucket by filename
    Uses specific functions for PDF and other file types
    """
    try:
        # Determine file extension
        file_extension = filename.split('.')[-1].lower()
        
        # Choose retrieval method based on file extension
        if file_extension == 'pdf':
            # Use PDF-specific retrieval function
            file_content = get_pdf_from_s3(filename)
            content_type = 'application/pdf'
        else:
            # Use generic file retrieval for images and other files
            file_content = get_file_from_s3(filename)
            
            # Determine content type based on file extension
            content_types = {
                'jpg': 'image/jpeg',
                'jpeg': 'image/jpeg',
                'png': 'image/png',
                'gif': 'image/gif'
            }
            content_type = content_types.get(file_extension, 'application/octet-stream')
        
        # Return file as a streaming response
        return StreamingResponse(
            io.BytesIO(file_content), 
            media_type=content_type, 
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    
    except HTTPException as e:
        # Rethrow HTTP exceptions
        raise e
    except Exception as e:
        # Handle any unexpected errors
        logging.error(f"Download error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving file: {str(e)}")

# Main FastAPI App Configuration (example)
from fastapi import FastAPI

app = FastAPI()

# Include the router
app.include_router(router, prefix="/files")

# Optional: Add startup and shutdown events for S3 client
@app.on_event("startup")
async def startup_event():
    logging.info("Application is starting up...")

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Application is shutting down...")