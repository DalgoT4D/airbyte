#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Optional


@dataclass(frozen=True)
class KoboSelectField:
    field_type: str
    list_name: str


@dataclass(frozen=True)
class KoboLabelMaps:
    field_labels: Mapping[str, str]
    choice_labels: Mapping[str, Mapping[str, str]]
    select_fields: Mapping[str, KoboSelectField]


def _extract_label(label: Any, language_index: int, fallback: Optional[str] = None) -> Optional[str]:
    if isinstance(label, list):
        if 0 <= language_index < len(label):
            value = label[language_index]
            return value if isinstance(value, str) and value else fallback
        if label:
            value = label[0]
            return value if isinstance(value, str) and value else fallback
        return fallback
    if isinstance(label, str) and label:
        return label
    return fallback


def build_label_maps(asset: Mapping[str, Any], *, language_index: int = 0) -> KoboLabelMaps:
    content = asset.get("content") or {}
    survey = content.get("survey") or []
    choices = content.get("choices") or []

    field_labels: dict[str, str] = {}
    select_fields: dict[str, KoboSelectField] = {}

    for field in survey:
        if not isinstance(field, Mapping):
            continue
        name = field.get("$autoname") or field.get("name") or ""
        if not isinstance(name, str) or not name:
            continue

        label = _extract_label(field.get("label"), language_index, fallback=None)
        if label:
            field_labels[name] = label

        field_type = field.get("type") or ""
        if isinstance(field_type, str) and ("select_one" in field_type or "select_multiple" in field_type):
            list_name = field.get("select_from_list_name") or ""
            if not list_name and isinstance(field_type, str):
                parts = field_type.split()
                if len(parts) >= 2:
                    list_name = parts[1]
            if isinstance(list_name, str) and list_name:
                select_fields[name] = KoboSelectField(field_type=field_type, list_name=list_name)

    choice_labels: dict[str, dict[str, str]] = {}
    for choice in choices:
        if not isinstance(choice, Mapping):
            continue
        list_name = choice.get("list_name")
        name = choice.get("name")
        if not isinstance(list_name, str) or not list_name or name is None:
            continue
        label = _extract_label(choice.get("label"), language_index, fallback=str(name))
        choice_labels.setdefault(list_name, {})[str(name)] = label or str(name)

    return KoboLabelMaps(field_labels=field_labels, choice_labels=choice_labels, select_fields=select_fields)


def label_submission(submission: Mapping[str, Any], maps: KoboLabelMaps) -> MutableMapping[str, Any]:
    labeled: dict[str, Any] = {}
    for key, value in submission.items():
        if not isinstance(key, str):
            labeled[str(key)] = value
            continue

        short_key = key.split("/")[-1]
        field_label = maps.field_labels.get(short_key, key)

        select_field = maps.select_fields.get(short_key)
        if select_field and value not in (None, ""):
            choices_for_list = maps.choice_labels.get(select_field.list_name, {})
            if "select_multiple" in select_field.field_type:
                parts = str(value).split()
                value = " ".join(choices_for_list.get(part, part) for part in parts)
            else:
                value = choices_for_list.get(str(value), value)

        labeled[field_label] = value

    return labeled

