# -*- coding: utf-8 -*-
"""
Genera un JSON consolidado para el dashboard de vacunas programadas.

Uso recomendado:
1) Guarda este archivo .py en la misma carpeta donde están tus Excel anuales:
   2023.xlsx, 2024.xlsx, 2025.xlsx, 2026.xlsx, etc.
2) Ejecuta:
   python crear_json_vacunas_programadas.py
3) Se generará:
   vacunas_programadas.json

El script:
- lee todos los .xlsx de la carpeta actual
- intenta procesar hojas como HISTORICO, ACTUALIZAR o cualquier hoja con las columnas esperadas
- consolida los registros
- elimina duplicados exactos por paciente/evento usando RUT + vacuna + dosis + fecha
- NO expone RUT ni nombre en el JSON final
"""

from __future__ import annotations
import json
import os
import re
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

MONTH_MAP = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4, "MAYO": 5, "JUNIO": 6,
    "JULIO": 7, "AGOSTO": 8, "SEPTIEMBRE": 9, "SETIEMBRE": 9, "OCTUBRE": 10,
    "NOVIEMBRE": 11, "DICIEMBRE": 12,
}
MONTH_NAMES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre",
    12: "Diciembre",
}


def normalize_text(value: Any) -> Optional[str]:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return re.sub(r"\s+", " ", text)


def parse_date(value: Any, mes_num: Optional[int] = None, anio: Optional[int] = None, dia: Optional[int] = None) -> Optional[datetime]:
    if value is None or (isinstance(value, float) and pd.isna(value)) or str(value).strip() == "":
        if anio and mes_num and dia:
            try:
                return datetime(int(anio), int(mes_num), int(dia))
            except Exception:
                return None
        return None

    if isinstance(value, datetime):
        return value

    text = str(value).strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%y", "%d/%m/%y"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            pass

    parsed = pd.to_datetime(value, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def choose_column(columns: Dict[str, str], *candidates: str) -> Optional[str]:
    for candidate in candidates:
        if candidate in columns:
            return columns[candidate]
    return None


def process_workbook(filepath: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    try:
        xls = pd.ExcelFile(filepath)
    except Exception as exc:
        print(f"[ADVERTENCIA] No se pudo abrir {os.path.basename(filepath)}: {exc}")
        return records

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(filepath, sheet_name=sheet_name)
        except Exception as exc:
            print(f"[ADVERTENCIA] No se pudo leer la hoja '{sheet_name}' en {os.path.basename(filepath)}: {exc}")
            continue

        if df.empty or len(df.columns) == 0:
            continue

        cols = {str(c).upper().strip(): c for c in df.columns}

        fecha_col = choose_column(cols, "FECHA", "COLUMN1", "FEC_ATENCION")
        mes_col = choose_column(cols, "MES")
        anio_col = choose_column(cols, "ANO", "AÑO", "ANIO", "ANO3")
        dia_col = choose_column(cols, "DIAS", "DIA")
        inmun_col = choose_column(cols, "INMUNIZACION ADMINISTRADA", "INMUNIZACION_ADNMINISTRADA", "INMUNIZACION_ADMINISTRADA")
        dosis_col = choose_column(cols, "DOSIS")
        criterio_col = choose_column(cols, "CRITERIO ELEGIBILIDAD", "CRITERIO_ELEGIBILIDAD")
        establecimiento_col = choose_column(cols, "ESTABLECIMIENTO")
        comuna_col = choose_column(cols, "COMUNA")
        lote_col = choose_column(cols, "LOTE")
        rut_col = choose_column(cols, "RUT")
        nombre_col = choose_column(cols, "NOMBRE_COMPLETO")

        enough_columns = (
            ((mes_col and anio_col) or fecha_col) and inmun_col and dosis_col and criterio_col
        )
        if not enough_columns:
            continue

        for _, row in df.iterrows():
            mes_texto = normalize_text(row[mes_col]) if mes_col else None
            anio = row[anio_col] if anio_col else None
            mes_num = MONTH_MAP.get(str(mes_texto).upper(), None) if mes_texto else None
            dia = row[dia_col] if dia_col and pd.notna(row[dia_col]) else None
            fecha = parse_date(row[fecha_col], mes_num, anio, dia) if fecha_col else parse_date(None, mes_num, anio, dia)

            record = {
                "archivo_origen": os.path.basename(filepath),
                "hoja_origen": sheet_name,
                "fecha": fecha.strftime("%Y-%m-%d") if fecha else None,
                "anio": int(fecha.year) if fecha else (int(float(anio)) if pd.notna(anio) else None),
                "mes_num": int(fecha.month) if fecha else mes_num,
                "mes": MONTH_NAMES_ES.get(int(fecha.month)) if fecha else (MONTH_NAMES_ES.get(mes_num) if mes_num else (mes_texto.title() if mes_texto else None)),
                "dia": int(fecha.day) if fecha else (int(float(dia)) if dia is not None and pd.notna(dia) else None),
                "rut": normalize_text(row[rut_col]) if rut_col else None,
                "nombre_completo": normalize_text(row[nombre_col]) if nombre_col else None,
                "comuna": normalize_text(row[comuna_col]) if comuna_col else None,
                "establecimiento": normalize_text(row[establecimiento_col]) if establecimiento_col else None,
                "inmunizacion_administrada": normalize_text(row[inmun_col]) if inmun_col else None,
                "dosis": normalize_text(row[dosis_col]) if dosis_col else None,
                "criterio_elegibilidad": normalize_text(row[criterio_col]) if criterio_col else None,
                "lote": normalize_text(row[lote_col]) if lote_col else None,
            }

            if record["inmunizacion_administrada"] or record["dosis"] or record["criterio_elegibilidad"]:
                records.append(record)

    return records


def deduplicate(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique_records: List[Dict[str, Any]] = []
    seen = set()

    for r in records:
        key = (
            r["fecha"],
            r["rut"],
            r["nombre_completo"],
            r["establecimiento"],
            r["inmunizacion_administrada"],
            r["dosis"],
            r["criterio_elegibilidad"],
            r["lote"],
        )
        if key in seen:
            continue
        seen.add(key)
        unique_records.append(r)

    unique_records.sort(key=lambda x: (x["fecha"] or "", x["inmunizacion_administrada"] or "", x["dosis"] or ""))
    return unique_records


def build_public_json(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    public_records = []
    for r in records:
        public_records.append({
            "archivo_origen": r["archivo_origen"],
            "hoja_origen": r["hoja_origen"],
            "fecha": r["fecha"],
            "anio": r["anio"],
            "mes_num": r["mes_num"],
            "mes": r["mes"],
            "dia": r["dia"],
            "comuna": r["comuna"],
            "establecimiento": r["establecimiento"],
            "inmunizacion_administrada": r["inmunizacion_administrada"],
            "dosis": r["dosis"],
            "criterio_elegibilidad": r["criterio_elegibilidad"],
            "lote": r["lote"],
        })

    total = len(public_records)
    years = sorted({r["anio"] for r in public_records if r["anio"] is not None})
    month_tuples = sorted({(r["anio"], r["mes_num"], r["mes"]) for r in public_records if r["anio"] and r["mes_num"]})

    inmun_counts = Counter(r["inmunizacion_administrada"] for r in public_records if r["inmunizacion_administrada"])
    dosis_counts = Counter(r["dosis"] for r in public_records if r["dosis"])
    criterio_counts = Counter(r["criterio_elegibilidad"] for r in public_records if r["criterio_elegibilidad"])
    month_counts = Counter((r["anio"], r["mes_num"], r["mes"]) for r in public_records if r["anio"] and r["mes_num"])
    day_counts = Counter(r["fecha"] for r in public_records if r["fecha"])

    return {
        "metadata": {
            "generado_en": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_registros": total,
            "anios_disponibles": years,
            "meses_disponibles": [{"anio": a, "mes_num": m, "mes": mes} for a, m, mes in month_tuples],
            "fuentes_detectadas": sorted({r["archivo_origen"] for r in public_records}),
        },
        "catalogos": {
            "inmunizaciones": sorted(k for k in inmun_counts),
            "dosis": sorted(k for k in dosis_counts),
            "criterios": sorted(k for k in criterio_counts),
            "meses_orden": [MONTH_NAMES_ES[i] for i in range(1, 13)],
        },
        "resumen": {
            "top_inmunizaciones": [{"nombre": k, "total": v} for k, v in inmun_counts.most_common(15)],
            "top_dosis": [{"nombre": k, "total": v} for k, v in dosis_counts.most_common(15)],
            "top_criterios": [{"nombre": k, "total": v} for k, v in criterio_counts.most_common(15)],
            "serie_mensual": [
                {"anio": a, "mes_num": m, "mes": mes, "total": c}
                for (a, m, mes), c in sorted(month_counts.items(), key=lambda x: (x[0][0], x[0][1]))
            ],
            "serie_diaria": [{"fecha": fecha, "total": total} for fecha, total in sorted(day_counts.items())],
        },
        "registros": public_records,
    }


def main() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(base_dir, "vacunas_programadas.json")

    excel_files = [
        f for f in os.listdir(base_dir)
        if f.lower().endswith(".xlsx") and not f.startswith("~$")
    ]

    if not excel_files:
        print("No se encontraron archivos .xlsx en la carpeta del script.")
        return

    all_records: List[Dict[str, Any]] = []
    for filename in sorted(excel_files):
        file_path = os.path.join(base_dir, filename)
        records = process_workbook(file_path)
        all_records.extend(records)
        print(f"[OK] {filename}: {len(records)} registros leídos")

    consolidated = deduplicate(all_records)
    payload = build_public_json(consolidated)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print()
    print(f"JSON generado correctamente: {output_path}")
    print(f"Registros consolidados: {len(consolidated)}")
    print(f"Años detectados: {payload['metadata']['anios_disponibles']}")


if __name__ == "__main__":
    main()
