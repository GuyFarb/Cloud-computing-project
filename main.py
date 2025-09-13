import os
import re
import functions_framework
from google.cloud import firestore

# ====== OPTIONAL: Gemini for nicer titles ======
USE_GEMINI = False
try:
    import google.generativeai as genai
    api_key = os.getenv("GEMINI_API_KEY")
    if api_key:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-1.5-flash")
        USE_GEMINI = True
except Exception:
    USE_GEMINI = False
# ===============================================

# טיפוסי האירוע של Eventarc (Firestore) – לפריסת Protobuf
from google.events.cloud.firestore.v1 import data as firestore_events
from google.protobuf.json_format import MessageToDict


def _simple_title(text: str) -> str:
    if not text:
        return "שאלה"
    words = re.findall(r"\w+|\S", text)
    return " ".join(words[:6]).strip().rstrip("?:!.,;")[:60]


def _gemini_title(text: str) -> str:
    try:
        rsp = model.generate_content(
            "צור כותרת קצרה וברורה (עד 6 מילים) לשאלה הבאה:\n\n" + (text or "")
        )
        t = (rsp.text or "").strip()
        return t[:60] or _simple_title(text)
    except Exception:
        return _simple_title(text)


def make_title(text: str) -> str:
    return _gemini_title(text) if USE_GEMINI else _simple_title(text)


def _doc_path_from_name(name: str) -> str:
    # name לדוגמה:
    # projects/PROJECT_ID/databases/(default)/documents/questions/abc123
    if not name:
        return ""
    parts = name.split("/documents/")
    return parts[-1] if len(parts) >= 2 else ""


@functions_framework.cloud_event
def hello_firestore(event):
    """
    Entry point (לשים כ-Function entry point): hello_firestore
    מקבל CloudEvent בפורמט Protobuf מ-Eventarc Firestore.
    """

    # event.data הוא bytes של Protobuf -> DocumentEventData
    doc_event = firestore_events.DocumentEventData()
    doc_event.ParseFromString(event.data)

    # אפשר לעבוד ישירות מול המסר, או להפוך ל-dict לנוחות:
    doc_dict = MessageToDict(doc_event, preserving_proto_field_name=True)

    # שליפת המסמך והנתיבים
    value = doc_dict.get("value", {})
    name = value.get("name", "")
    doc_path = _doc_path_from_name(name)

    # נטפל רק במסמכים ב-root של 'questions'
    if not doc_path.startswith("questions/"):
        print(f"Skip non-questions path: {doc_path}")
        return "ignored"

    # שליפת השדה question (אם קיים)
    fields = value.get("fields", {})
    question = (fields.get("question", {}) or {}).get("stringValue", "")

    title = make_title(question)

    # כתיבה חזרה ל-Firestore
    db = firestore.Client()
    db.document(doc_path).set({"title": title}, merge=True)
    print(f"Updated {doc_path} with title='{title}'")
    return "ok"
