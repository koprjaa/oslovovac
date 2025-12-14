
"""
Project: Vocative Generator
File: src/models.py
Description: Data models defining successful and failed name processing results.
Author: Jan Alexandr Kopřiva jan.alexandr.kopriva@gmail.com
License: MIT
"""
from dataclasses import dataclass
from typing import Optional, Tuple

@dataclass
class NameResult:
    original_name: str
    vocative: str
    first_name: str
    surname: str
    success: bool
    error_message: Optional[str] = None

    @classmethod
    def from_vocative(cls, original_name: str, vocative: str) -> 'NameResult':
        first_name, surname = cls.split_vocative(vocative)
        # Success if vocative differs from original and is not empty
        success = bool(vocative and vocative.strip().lower() != original_name.strip().lower())
        return cls(
            original_name=original_name,
            vocative=vocative.strip() if vocative else original_name, # Ensure vocative is not None
            first_name=first_name,
            surname=surname,
            success=success
        )

    @classmethod
    def error(cls, original_name: str, error_message: str) -> 'NameResult':
        return cls(
            original_name=original_name,
            vocative=original_name, # Return original on error
            first_name='',
            surname='',
            success=False,
            error_message=error_message
        )

    @staticmethod
    def split_vocative(vocative: str) -> Tuple[str, str]:
        if not vocative:
            return "", ""
        parts = vocative.strip().split()
        if not parts: # If vocative is just whitespace
            return "", ""
        if len(parts) <= 1:
            return parts[0], ""
        surname = parts[-1]
        first_names = " ".join(parts[:-1])
        return first_names, surname
