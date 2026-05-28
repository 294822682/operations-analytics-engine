from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ApiError(Exception):
    code: str
    message: str
    status_code: int = 400
    details: Dict[str, Any] = field(default_factory=dict)


class InvalidReportDateError(ApiError):
    def __init__(self, report_date: str) -> None:
        super().__init__(
            code="INVALID_REPORT_DATE",
            message="Invalid report_date, expected a real calendar date in YYYY-MM-DD format",
            status_code=400,
            details={
                "report_date": report_date,
                "expected_format": "YYYY-MM-DD",
            },
        )


class ExecutionAlreadyRunningError(ApiError):
    def __init__(self, details: Dict[str, Any]) -> None:
        super().__init__(
            code="EXECUTION_ALREADY_RUNNING",
            message="Another execution is currently running",
            status_code=409,
            details=details,
        )


def build_error_response(code: str, message: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
        }
    }
