from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
import os
import base64
from email.message import EmailMessage
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials 
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Constants pass your own data  .
SCOPES = ['Google Scope URL']
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'credentials.json'

app = FastAPI()

class EmailSchema(BaseModel):
    to: EmailStr
    subject: str
    body: str

def get_gmail_service():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)

def send_email_via_gmail(to_email: str, subject: str, body: str):
    service = get_gmail_service()

    message = EmailMessage()
    message.set_content(body)

    message['To'] = to_email
    message['From'] = to_email  # Can be changed if using Workspace
    message['Subject'] = subject

    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    create_message = {'raw': encoded_message}

    send_message = service.users().messages().send(userId="me", body=create_message).execute()
    return send_message['id']

@app.post("/send-email")
def send_email(data: EmailSchema):
    try:
        message_id = send_email_via_gmail(data.to, data.subject, data.body)
        return {"status": "success", "message_id": message_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
