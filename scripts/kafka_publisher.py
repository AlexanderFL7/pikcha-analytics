# scripts/kafka_publisher.py
import os, json, re, hmac, hashlib
from kafka import KafkaProducer
from pathlib import Path

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MASK_KEY = os.getenv("KAFKA_MASK_KEY", "super-secret-key")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    api_version=(3, 0, 0)
)

def normalize_phone(phone):
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    return ("+" + digits) if digits else ""

def mask_value(value):
    v = (value or "").strip().lower()
    return hmac.new(MASK_KEY.encode(), v.encode(), hashlib.sha256).hexdigest()

def process_and_send(path: Path, topic: str):
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    # mask top-level customer/email/phone if present
    if "email" in obj:
        obj["email_masked"] = mask_value(obj.pop("email"))
    if "phone" in obj:
        obj["phone_masked"] = mask_value(normalize_phone(obj.pop("phone")))
    # nested customer in purchases
    if isinstance(obj.get("customer"), dict):
        cust = obj["customer"]
        if "email" in cust:
            cust["email_masked"] = mask_value(cust.pop("email"))
        if "phone" in cust:
            cust["phone_masked"] = mask_value(normalize_phone(cust.pop("phone")))
    producer.send(topic, obj)
    print(f"Sent {path.name} -> {topic}")

if __name__ == "__main__":
    base = Path.cwd() / "data"
    for sub in ["stores", "products", "customers", "purchases"]:
        dirp = base / sub
        if not dirp.exists(): 
            print("Dir missing:", dirp); continue
        for p in dirp.glob("*.json"):
            process_and_send(p, sub)
    producer.flush()
    print("✅ All files published to Kafka")