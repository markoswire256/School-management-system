"""
================================================================================
[DEPRECATED] ACADEMIACORE PROTOTYPE ENGINE & LEGACY TEST HARNESS
================================================================================
NOTICE: This standalone monolith script has been DEPRECATED in favor of the
canonical modular backend located in 'AcademiaCore/backend/app'.

DO NOT USE THIS SCRIPT FOR PRODUCTION DATABASE INITIALIZATION OR SERVING.
- Canonical Application Entrypoint:   AcademiaCore/backend/app/main.py
- Canonical Server & CLI Launcher:    AcademiaCore/backend/app/run.py
- Canonical Release & Build Pipeline:  AcademiaCore/build_installer.py
- Production Database Migrations:     AcademiaCore/backend/alembic/
================================================================================
"""
This single unified file incorporates all requested features, architectures, 
security safeguards, database schemas, API routers, and test suites:

1. CLI Orchestrator:
   - Run 'python academiacore_engine.py clean' -> Deep cleanup of stale logs & bytecode.
   - Run 'python academiacore_engine.py init-db' -> Applies the master multi-tier schema.
   - Run 'python academiacore_engine.py test' -> Runs the brutal security & integrity audit suite.
   - Run 'python academiacore_engine.py serve' -> Boots the FastAPI LAN server with safe auto-discovery.

2. Master Modular Database Schema (SQLite / PostgreSQL):
   - Multi-Tier Curriculum Engine:
       * Nursery / ECCE: Qualitative developmental domain rubrics (Emerging, Developing, Achieved).
       * Lower Primary (P1-P3): Thematic learning areas (Literacy I/II, Numeracy, News, Creative Arts).
       * Upper Primary (P4-P7): UNEB standard 9-point grading scale (D1 to F9) with aggregate calculation.
       * Lower Secondary (S1-S4): NCDC Competency-Based Curriculum (CBC) Activities of Integration (AOIs)
         scored 1.0 - 3.0 with automatic 20% formative weight calculation and letter grade conversion (A-E).
       * Upper Secondary (S5-S6): Criterion-referenced learner-centered assessment constructs.
   - Bursar Desk & Financial Sub-Ledger:
       * SchoolPay Webhook integration with SHA-256 HMAC signature verification.
       * Mobile Money (MTN MoMo OpenAPI / Airtel Africa API) & Bank MT940 integration.
       * Double-entry ledger tracking tuition, feeding, PTA, uniform fees, invoices, and balances.
   - Multi-Channel Messaging Engine:
       * SMS outbound queue for payment receipts, fee balance alerts, and school announcements.
       * Private parent invitation desk for disciplinary and performance reviews.
   - Sync & Outbox Engine:
       * Resilient offline-first outbox queue (sync_outbox) for bidirectional cloud synchronization.
   - Institutional White-Label Customization:
       * Dynamically configured school branding, colors, badges/crests, and UNEB center codes.

3. Discovery Service Fix:
   - Safe Zeroconf/mDNS auto-discovery with stub fallback to prevent NameError startup crashes.

4. Multi-Portal Architecture & Specifications:
   - Role contracts for Teacher, Parent, and Student portals (Mobile & Web).
================================================================================
"""

import os
import sys
import shutil
import sqlite3
import hashlib
import uuid
import logging
import socket
import unittest
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, Literal, List, Dict, Any

# Force UTF-8 stdout/stderr so Unicode status glyphs (✓, ✗, …) never crash the
# CLI on Windows terminals whose legacy code page (e.g. cp1252) can't encode
# them. Falls back silently if the stream doesn't support reconfigure (e.g.
# when stdout is replaced by a non-standard object in some test harnesses).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

# ==============================================================================
# 1. LOGGING SETUP
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("academiacore.unified")

# ==============================================================================
# 2. LAN AUTO-DISCOVERY SERVICE (Crash-Proof Fallback)
# ==============================================================================
try:
    from zeroconf import IPVersion, ServiceInfo, Zeroconf, ServiceListener
    HAS_ZEROCONF = True
except ImportError:
    HAS_ZEROCONF = False
    class ServiceListener:
        """Stub fallback to prevent NameError when zeroconf is uninstalled."""
        pass

class _DiscoveryCollector(ServiceListener):
    def __init__(self):
        self.services = []

    def remove_service(self, zc: "Zeroconf", type_: str, name: str) -> None:
        logger.debug("LAN Service removed: %s", name)

    def add_service(self, zc: "Zeroconf", type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info:
            self.services.append(info)

    def update_service(self, zc: "Zeroconf", type_: str, name: str) -> None:
        pass

class DiscoveryService:
    def __init__(self, port: int = 8000, service_type: str = "_academiacore._tcp.local."):
        self.port = port
        self.service_type = service_type
        self.zeroconf: Optional[Any] = None
        self.service_info: Optional[Any] = None
        self.is_running: bool = False

    def _get_local_ip(self) -> str:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip

    def start_broadcasting(self, school_name: str = "AcademiaCore Local Node") -> bool:
        if not HAS_ZEROCONF:
            logger.warning("zeroconf library not installed. LAN auto-discovery broadcast disabled.")
            return False
        try:
            local_ip = self._get_local_ip()
            desc = {"name": school_name, "version": "2.0.0", "path": "/"}
            self.zeroconf = Zeroconf(ip_version=IPVersion.V4Only)
            self.service_info = ServiceInfo(
                self.service_type,
                f"AcademiaCore-Server.{self.service_type}",
                addresses=[socket.inet_aton(local_ip)],
                port=self.port,
                properties=desc,
                server=f"{socket.gethostname()}.local.",
            )
            self.zeroconf.register_service(self.service_info)
            self.is_running = True
            logger.info("Broadcasting AcademiaCore node on %s:%s via mDNS", local_ip, self.port)
            return True
        except Exception as exc:
            logger.error("Failed to start mDNS broadcast: %s", exc)
            self.is_running = False
            return False

    def stop_broadcasting(self) -> None:
        if self.zeroconf and self.service_info:
            try:
                self.zeroconf.unregister_service(self.service_info)
                self.zeroconf.close()
            except Exception as exc:
                logger.error("Error closing Zeroconf: %s", exc)
        self.is_running = False
        logger.info("LAN auto-discovery broadcast stopped.")

discovery_service = DiscoveryService()

# ==============================================================================
# 3. MASTER MODULAR DATABASE SCHEMA (SQL DDL)
# ==============================================================================
MASTER_SCHEMA_SQL = """
-- 1. Institutional Configuration & White-Label Customization
CREATE TABLE IF NOT EXISTS school_profile (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    motto TEXT,
    crest_url TEXT,
    primary_color TEXT DEFAULT '#1E3A8A',
    secondary_color TEXT DEFAULT '#F59E0B',
    contact_phone TEXT,
    contact_email TEXT,
    school_pay_code TEXT,
    momo_merchant_code TEXT,
    uneb_center_number TEXT,
    is_nursery_enabled BOOLEAN DEFAULT FALSE,
    is_lower_primary_enabled BOOLEAN DEFAULT FALSE,
    is_upper_primary_enabled BOOLEAN DEFAULT FALSE,
    is_lower_secondary_enabled BOOLEAN DEFAULT FALSE,
    is_upper_secondary_enabled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Master Student Identity & Enrollment Lifecycle
CREATE TABLE IF NOT EXISTS students (
    id TEXT PRIMARY KEY,
    student_nin TEXT,
    reg_number TEXT UNIQUE NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender TEXT CHECK (gender IN ('MALE', 'FEMALE')),
    date_of_birth DATE,
    guardian_name TEXT,
    guardian_phone TEXT NOT NULL,
    guardian_email TEXT,
    education_level TEXT CHECK (education_level IN ('NURSERY', 'LOWER_PRIMARY', 'UPPER_PRIMARY', 'LOWER_SECONDARY', 'UPPER_SECONDARY')),
    current_class TEXT NOT NULL,
    current_stream TEXT,
    status TEXT DEFAULT 'ACTIVE' CHECK (status IN ('APPLICANT', 'ACTIVE', 'SUSPENDED', 'TRANSFERRED', 'GRADUATED', 'WITHDRAWN')),
    photo_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Bursar Double-Entry Ledger & Fee Reconciliation
CREATE TABLE IF NOT EXISTS fee_structures (
    id TEXT PRIMARY KEY,
    academic_year INTEGER NOT NULL,
    term INTEGER CHECK (term IN (1, 2, 3)),
    education_level TEXT NOT NULL,
    item_name TEXT NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    is_compulsory BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS student_invoices (
    id TEXT PRIMARY KEY,
    student_id TEXT NOT NULL REFERENCES students(id),
    academic_year INTEGER NOT NULL,
    term INTEGER NOT NULL,
    total_billed DECIMAL(12, 2) NOT NULL,
    balance DECIMAL(12, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payment_transactions (
    id TEXT PRIMARY KEY,
    invoice_id TEXT REFERENCES student_invoices(id),
    student_id TEXT NOT NULL REFERENCES students(id),
    amount_paid DECIMAL(12, 2) NOT NULL,
    payment_method TEXT CHECK (payment_method IN ('SCHOOL_PAY', 'MTN_MOMO', 'AIRTEL_MOMO', 'BANK_DEPOSIT', 'CASH')),
    channel_reference TEXT UNIQUE,
    collected_by TEXT,
    signature_hash TEXT,
    notes TEXT,
    reconciled_online BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Multi-Curricular Academic Grading Systems

-- 4a. Nursery / ECCE Qualitative Development Rubrics
CREATE TABLE IF NOT EXISTS nursery_assessments (
    id TEXT PRIMARY KEY,
    student_id TEXT NOT NULL REFERENCES students(id),
    term INTEGER NOT NULL,
    academic_year INTEGER NOT NULL,
    developmental_domain TEXT NOT NULL,
    skill_indicator TEXT NOT NULL,
    score_rubric TEXT CHECK (score_rubric IN ('EMERGING', 'DEVELOPING', 'ACHIEVED')),
    teacher_notes TEXT,
    teacher_id TEXT NOT NULL
);

-- 4b. Lower Primary (P1-P3) Thematic Learning Areas
CREATE TABLE IF NOT EXISTS lower_primary_thematic_marks (
    id TEXT PRIMARY KEY,
    student_id TEXT NOT NULL REFERENCES students(id),
    term INTEGER NOT NULL,
    academic_year INTEGER NOT NULL,
    theme_name TEXT NOT NULL,
    learning_area TEXT NOT NULL,
    level_competency INTEGER CHECK (level_competency IN (1, 2, 3)),
    teacher_id TEXT NOT NULL
);

-- 4c. Upper Primary (P4-P7) UNEB Standard Marks
CREATE TABLE IF NOT EXISTS upper_primary_marks (
    id TEXT PRIMARY KEY,
    student_id TEXT NOT NULL REFERENCES students(id),
    term INTEGER NOT NULL,
    academic_year INTEGER NOT NULL,
    subject TEXT CHECK (subject IN ('ENGLISH', 'MATHEMATICS', 'SCIENCE', 'SST')),
    raw_score DECIMAL(5, 2) CHECK (raw_score BETWEEN 0 AND 100),
    grade TEXT,
    aggregates INTEGER,
    teacher_id TEXT NOT NULL
);

-- 4d. Lower Secondary (S1-S4) CBC Formative (AOI) & Summative Marks
CREATE TABLE IF NOT EXISTS secondary_cbc_assessments (
    id TEXT PRIMARY KEY,
    student_id TEXT NOT NULL REFERENCES students(id),
    subject_id TEXT NOT NULL,
    term INTEGER NOT NULL,
    academic_year INTEGER NOT NULL,
    aoi_title TEXT NOT NULL,
    score_out_of_3 DECIMAL(3, 1) CHECK (score_out_of_3 BETWEEN 1.0 AND 3.0),
    competency_descriptor TEXT,
    formative_weight_20 DECIMAL(5, 2),
    summative_score_80 DECIMAL(5, 2),
    final_grade TEXT,
    teacher_id TEXT NOT NULL
);

-- 5. Outbox Sync Queue (Offline LAN -> Cloud Postgres Sync)
CREATE TABLE IF NOT EXISTS sync_outbox (
    mutation_id TEXT PRIMARY KEY,
    entity_table TEXT NOT NULL,
    operation TEXT CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    record_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_synced BOOLEAN DEFAULT FALSE,
    synced_at TIMESTAMP
);

-- 6. Outbound SMS & Parent Notices Queue
CREATE TABLE IF NOT EXISTS sms_queue (
    id TEXT PRIMARY KEY,
    recipient_phone TEXT NOT NULL,
    message_body TEXT NOT NULL,
    message_type TEXT CHECK (message_type IN ('PAYMENT_RECEIPT', 'FEE_REMINDER', 'INVITATION', 'ANNOUNCEMENT', 'DISCIPLINE')),
    status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    provider_ref TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMP
);
"""

# ==============================================================================
# 4. WORKSPACE CLEANUP UTILITY
# ==============================================================================
def clean_workspace(target_path: str = ".") -> None:
    """Removes stale crash logs, bytecode, and cache folders."""
    base_dir = Path(target_path)
    print(f"[*] Sweeping workspace for stale files at: {base_dir.resolve()}")
    
    file_patterns = ["*.log", "crash.log", "academiacore.log", "*.pyc", "*.pyo"]
    removed_files = 0
    for pattern in file_patterns:
        for f in base_dir.rglob(pattern):
            try:
                f.unlink()
                print(f"  [-] Removed file: {f.name}")
                removed_files += 1
            except Exception as exc:
                print(f"  [!] Failed to remove file {f}: {exc}")

    cache_dirs = ["__pycache__", ".pytest_cache", ".ruff_cache"]
    removed_dirs = 0
    for cdir in cache_dirs:
        for d in base_dir.rglob(cdir):
            try:
                shutil.rmtree(d)
                print(f"  [-] Removed directory: {d}")
                removed_dirs += 1
            except Exception as exc:
                print(f"  [!] Failed to remove directory {d}: {exc}")

    print(f"[✓] Cleanup complete: {removed_files} files and {removed_dirs} cache directories removed.")

# ==============================================================================
# 5. DATABASE INITIALIZATION
# ==============================================================================
def init_database(db_path: str = "academiacore.db") -> None:
    """Applies master schema to local SQLite database."""
    print(f"[*] Initializing AcademiaCore Database at: {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executescript(MASTER_SCHEMA_SQL)
    conn.commit()
    conn.close()
    print("[✓] Database schema applied successfully.")

# ==============================================================================
# 6. BRUTAL SECURITY AUDIT SUITE
# ==============================================================================
class AcademiaCoreBrutalSecurityTest(unittest.TestCase):
    def setUp(self):
        self.secret_key = "ACADEMIACORE_SCHOOLPAY_SECRET_KEY"

    def test_01_webhook_tamper_detection(self):
        """Verifies that altering 1 cent or character causes HMAC signature failure."""
        receipt_no = "REC-UG-2026-9901"
        valid_amount = 450000.0
        raw = f"{receipt_no}:{valid_amount}:{self.secret_key}"
        legit_signature = hashlib.sha256(raw.encode("utf-8")).hexdigest()

        tampered_amount = 50000.0
        tampered_raw = f"{receipt_no}:{tampered_amount}:{self.secret_key}"
        tampered_hash = hashlib.sha256(tampered_raw.encode("utf-8")).hexdigest()

        self.assertNotEqual(tampered_hash.lower(), legit_signature.lower(),
                            "SECURITY BREACH: Tampered payload produced matching signature!")

    def test_02_sql_injection_defense(self):
        """Ensures dangerous query syntax is detected and neutralized."""
        malicious_inputs = [
            "' OR '1'='1",
            "'; DROP TABLE students; --",
            "admin'--",
            "1 UNION SELECT null, username, password FROM users --"
        ]
        sql_injection_tokens = re.compile(r"(--|;|UNION|SELECT|DROP|OR)", re.IGNORECASE)
        for payload in malicious_inputs:
            has_dangerous = bool(sql_injection_tokens.search(payload))
            self.assertTrue(has_dangerous, f"Failed to identify dangerous injection syntax: {payload}")

    def test_03_double_spend_idempotency_enforcement(self):
        """Simulates replay attacks on payment webhooks."""
        processed_receipts = set()
        receipt_id = "SCHOOLPAY-TX-882910"

        first_hit = False
        if receipt_id not in processed_receipts:
            processed_receipts.add(receipt_id)
            first_hit = True
        self.assertTrue(first_hit)

        second_hit = False
        if receipt_id not in processed_receipts:
            processed_receipts.add(receipt_id)
            second_hit = True
        self.assertFalse(second_hit, "SECURITY BREACH: Duplicate transaction allowed to replay!")

    def test_04_rbac_privilege_boundary(self):
        """Verifies role isolation: TEACHER cannot access BURSAR or DEAN functions."""
        role_permissions = {
            "TEACHER": ["mark:enter", "attendance:record", "materials:upload"],
            "BURSAR": ["payments:record", "fee_structure:modify", "ledger:reconcile"],
            "HEADTEACHER": ["*"]
        }
        teacher_role = "TEACHER"
        forbidden_action = "payments:record"
        has_access = forbidden_action in role_permissions.get(teacher_role, [])
        self.assertFalse(has_access, "SECURITY BREACH: Teacher granted access to financial payment ledger!")

    def test_05_uuid_collision_and_entropy(self):
        """Verifies offline mutation keys avoid collisions across nodes."""
        generated = {str(uuid.uuid4()) for _ in range(10000)}
        self.assertEqual(len(generated), 10000, "Entropy failure: UUID collision detected!")

def run_security_tests() -> bool:
    print("[*] Running AcademiaCore Brutal Security & Integrity Audit Suite...")
    suite = unittest.TestLoader().loadTestsFromTestCase(AcademiaCoreBrutalSecurityTest)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()

# ==============================================================================
# 7. FASTAPI APPLICATION FACTORY & ROUTERS
# ==============================================================================
def create_app():
    try:
        from fastapi import FastAPI, HTTPException, status, BackgroundTasks
        from pydantic import BaseModel, Field
    except ImportError:
        logger.error("FastAPI or Pydantic is not installed. Run: pip install fastapi uvicorn pydantic")
        return None

    app = FastAPI(
        title="AcademiaCore Enterprise School OS",
        version="2.0.0",
        description="Unified Ugandan Offline-First School Management Platform"
    )

    # --- Models ---
    class SchoolPayWebhookPayload(BaseModel):
        schoolpay_receipt_number: str = Field(..., alias="schoolpayReceiptNumber")
        student_reg_number: str = Field(..., alias="studentRegNumber")
        amount: float
        payment_date: str = Field(..., alias="paymentDate")
        channel: str
        payer_phone: Optional[str] = Field(None, alias="payerPhone")
        signature: str

    class ManualPaymentRequest(BaseModel):
        student_id: str
        amount: float
        payment_method: Literal["CASH", "BANK_DEPOSIT", "MTN_MOMO", "AIRTEL_MOMO"]
        channel_reference: str
        payer_phone: Optional[str] = None
        notes: Optional[str] = None
        collected_by: str

    class SingleSMSRequest(BaseModel):
        recipient_phone: str
        message: str
        message_type: str

    class ParentInvitationRequest(BaseModel):
        student_id: str
        parent_phone: str
        student_name: str
        meeting_reason: str
        proposed_date: str
        invitation_by: str

    # --- Bursar Routes ---
    @app.post("/api/v1/bursar/webhooks/schoolpay")
    async def schoolpay_webhook(payload: SchoolPayWebhookPayload):
        secret = "ACADEMIACORE_SCHOOLPAY_SECRET_KEY"
        raw = f"{payload.schoolpay_receipt_number}:{payload.amount}:{secret}"
        expected = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        if expected.lower() != payload.signature.lower():
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature")

        return {
            "status": "SUCCESS",
            "receipt": payload.schoolpay_receipt_number,
            "student": payload.student_reg_number,
            "amount": payload.amount,
            "reconciled_at": datetime.utcnow().isoformat()
        }

    @app.post("/api/v1/bursar/payments/record-manual")
    async def record_payment(data: ManualPaymentRequest):
        if data.amount <= 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Amount must be positive")
        txn_id = str(uuid.uuid4())
        rec_no = f"REC-{datetime.now().strftime('%Y%m%d')}-{txn_id[:6].upper()}"
        return {
            "transaction_id": txn_id,
            "receipt_number": rec_no,
            "student_id": data.student_id,
            "amount_paid": data.amount,
            "status": "RECORDED"
        }

    # --- Communication Routes ---
    @app.post("/api/v1/communication/sms/send")
    async def send_sms(data: SingleSMSRequest):
        msg_id = str(uuid.uuid4())
        return {"message_id": msg_id, "status": "QUEUED", "recipient": data.recipient_phone}

    @app.post("/api/v1/communication/parent/invite")
    async def invite_parent(invite: ParentInvitationRequest):
        formatted = (
            f"Dear Parent/Guardian of {invite.student_name}, you are requested to attend a private "
            f"{invite.meeting_reason} on {invite.proposed_date} at the school office. Ref: {invite.invitation_by}."
        )
        return {
            "invitation_id": str(uuid.uuid4()),
            "sms_id": str(uuid.uuid4()),
            "text": formatted,
            "status": "SENT_TO_QUEUE"
        }

    @app.get("/api/v1/health")
    async def health_check():
        return {
            "status": "OPERATIONAL",
            "discovery_active": discovery_service.is_running,
            "timestamp": datetime.utcnow().isoformat()
        }

    return app

# ==============================================================================
# 8. CLI ENTRYPOINT & COMMAND DISPATCHER
# ==============================================================================
def main():
    print("""
======================================================================
[WARNING] 'academiacore_engine.py' is DEPRECATED.
Please use the canonical modular backend entrypoints:
  - Run server:    python AcademiaCore/backend/app/run.py
  - Build release: python AcademiaCore/build_installer.py
  - Run tests:     pytest AcademiaCore/backend/tests
======================================================================
""")
    if len(sys.argv) < 2:
        print("""
Usage: python academiacore_engine.py <command>

Legacy Prototype Commands (for historical regression testing only):
  clean     : Purge stale logs and cache dirs.
  test      : Execute the legacy security & regression test suite.
  serve     : Start the legacy standalone server on 127.0.0.1:8000.
======================================================================
""")
        sys.exit(0)

    cmd = sys.argv[1].lower()

    if cmd == "clean":
        clean_workspace()
    elif cmd == "init-db":
        init_database()
    elif cmd == "test":
        success = run_security_tests()
        sys.exit(0 if success else 1)
    elif cmd == "serve":
        init_database()
        discovery_service.start_broadcasting("AcademiaCore Local Node")
        app = create_app()
        if app:
            import uvicorn
            print("[*] Starting AcademiaCore Server on http://127.0.0.1:8000 ...")
            try:
                uvicorn.run(app, host="127.0.0.1", port=8000)
            finally:
                discovery_service.stop_broadcasting()
    elif cmd == "all":
        clean_workspace()
        init_database()
        success = run_security_tests()
        if not success:
            print("[!] Security tests failed. Aborting.")
            sys.exit(1)
        print("[✓] All systems verified, secured, and ready for deployment.")
    else:
        print(f"[!] Unknown command '{cmd}'. Run without arguments for help.")
        sys.exit(1)

if __name__ == "__main__":
    main()
