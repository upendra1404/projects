import os
import json
import argparse
import smtplib
from email.mime.text import MIMEText
from ultralytics import YOLO
from PIL import Image
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate

# --- YOLO MODEL ---
MODEL_PATH = os.getenv("YOLO_MODEL_PATH", "yolov8_contamination.pt")
model = YOLO(MODEL_PATH)

# --- LLM SETUP ---
llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)

prompt = PromptTemplate.from_template(
    template="""
You are an AI assistant classifying waste bags and providing risk assessment.

Detected counts:
- Recyclable items: {recyclable_count}
- Non-recyclable (contaminants): {non_recyclable_count}

Respond in valid JSON with the following structure:

{{
  "status": "Clean" or "Contaminated",
  "confidence": "<%, approximate reasoning>",
  "recommendation": "<action text>",
  "risk": "<describe potential risk if contaminants remain>",
  "mitigation": "<suggest mitigation to address the risk>"
}}
"""
)

chain = prompt | llm

# --- FUNCTIONS ---

def detect_contamination(image_path: str) -> dict:
    """Run YOLO model to count recyclable vs contaminant objects."""
    result = model(image_path)[0]
    labels = result.boxes.cls.cpu().numpy().astype(int)
    return {
        "recyclable": int((labels == 0).sum()),
        "non_recyclable": int((labels == 1).sum())
    }

def send_email_alert(to_email: str, subject: str, body: str):
    """Send an email alert to the user using SMTP (Gmail example)."""
    from_email = os.getenv("ALERT_EMAIL")        # your Gmail address
    password = os.getenv("ALERT_EMAIL_PASSWORD") # your Gmail app password (not normal password)

    if not from_email or not password:
        print("⚠️ Email credentials not set in environment variables.")
        return

    msg = MIMEText(body, "plain")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(from_email, password)
            server.sendmail(from_email, to_email, msg.as_string())
        print(f"✅ Alert email sent to {to_email}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

def classify_with_llm(counts: dict, alert_email: str = None) -> dict:
    """Use LLM to decide contamination status and recommend actions.
       Falls back to safe defaults if LLM fails."""
    formatted = {
        "recyclable_count": counts["recyclable"],
        "non_recyclable_count": counts["non_recyclable"]
    }

    try:
        resp = chain.invoke(formatted)
        return json.loads(resp)

    except json.JSONDecodeError:
        return {
            "status": "Unknown",
            "confidence": "0%",
            "recommendation": "AI returned invalid JSON",
            "risk": "Cannot classify due to invalid LLM response",
            "mitigation": "Check prompt or model response"
        }

    except Exception as e:
        error_message = str(e)
        print(f"❌ LLM failed: {error_message}")

        # Send alert email if email is provided
        if alert_email:
            send_email_alert(
                alert_email,
                "🚨 LLM Classification Failed",
                f"GPT could not classify the waste bag.\n\nError:\n{error_message}"
            )

        return {
            "status": "Unknown",
            "confidence": "0%",
            "recommendation": "Unable to classify due to LLM failure",
            "risk": "Classification skipped due to error",
            "mitigation": "Fix API key quota or check model availability"
        }

def process_image(image_path: str, alert_email: str = None) -> dict:
    """Detect items, classify contamination, and send alerts on failure."""
    try:
        counts = detect_contamination(image_path)
        classification = classify_with_llm(counts, alert_email=alert_email)
        return {"counts": counts, "classification": classification}

    except Exception as e:
        error_msg = f"🚨 ERROR: Processing failed for {image_path}\nReason: {e}"
        print(error_msg)
        if alert_email:
            send_email_alert(alert_email, "🚨 Waste Processing Model Failure", error_msg)
        return {"error": str(e)}


# --- MAIN SCRIPT ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Contamination classification for waste bag images")
    parser.add_argument("image", type=str, help="Path to waste bag image")
    parser.add_argument("--email", type=str, help="Email address for contamination alerts")
    args = parser.parse_args()

    output = process_image(args.image, alert_email=args.email)
    print(json.dumps(output, indent=2))
