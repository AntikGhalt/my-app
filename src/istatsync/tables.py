"""Summary workbooks for the household disposable income table.

Two files are rebuilt whenever one of their input datasets changes:

* tabelle/tabella_reddito_trimestrale.xlsx: quarters and half-years, picked
  from drop-down lists, built from the quarterly datasets;
* tabelle/tabella_reddito_annuale.xlsx: years, from the annual datasets.

Each workbook keeps the chain visible and recomputable in Excel:

    Dati     raw series from the latest edition (values, untouched)
    Serie    one row per period, aggregated from Dati with formulas
             (flows summed, ULA and population averaged, wealth at the end
             of the period; incomplete periods give n.d.)
    Tabella  the table, driven by the drop-down cells, all formulas
    Note     sources, editions and definitions
"""

from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from openpyxl import Workbook
from openpyxl.formatting.rule import CellIsRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.workbook.defined_name import DefinedName
from openpyxl.worksheet.datavalidation import DataValidation

log = logging.getLogger(__name__)

TABLE_DIR = "tabelle"
NUMBER = "#,##0.0;[Red]-#,##0.0"
FONT = "Arial"
INPUT_FILL = PatternFill("solid", fgColor="FFF2CC")
HEAD_FILL = PatternFill("solid", fgColor="DDEBF7")


@dataclass(frozen=True)
class Input:
    """One raw series in the Dati sheet."""

    col: str
    dataset: str
    filter: dict
    kind: str  # flow | avg | stock
    label: str


def _sector_account(dataset: str) -> list[Input]:
    items = [
        ("D1_C", "D1_C_W0", "Redditi da lavoro dipendente (risorse)"),
        ("B2A3G", "B2A3G_B_W0", "Risultato lordo di gestione e reddito misto"),
        ("D4_C", "D4T_C_W0", "Redditi da capitale (risorse)"),
        ("D4_D", "D4T_D_W0", "Redditi da capitale (impieghi)"),
        ("D5_C", "D5_C_W0", "Imposte correnti (risorse)"),
        ("D5_D", "D5_D_W0", "Imposte correnti (impieghi)"),
        ("D61_C", "D61_C_W0", "Contributi sociali netti (risorse)"),
        ("D61_D", "D61_D_W0", "Contributi sociali netti (impieghi)"),
        ("D62_C", "D62_C_W0", "Prestazioni sociali (risorse)"),
        ("D62_D", "D62_D_W0", "Prestazioni sociali (impieghi)"),
        ("D7_C", "D7_C_W0", "Altri trasferimenti correnti (risorse)"),
        ("D7_D", "D7_D_W0", "Altri trasferimenti correnti (impieghi)"),
        ("D8_C", "D8_C_W0", "Rettifica per variazione dei diritti pensionistici (risorse)"),
        ("D8_D", "D8_D_W0", "Rettifica per variazione dei diritti pensionistici (impieghi)"),
        ("B6G", "B6G_B_W0", "Reddito disponibile lordo (B6G, senza rettifica D8)"),
    ]
    return [Input(col, dataset, {"DATA_TYPE_AGGR": code}, "flow", lab) for col, code, lab in items]


_WEALTH = {"STO": "LE", "INSTR_ASSET": "F"}

QUARTERLY = [
    *_sector_account("famiglie_conto_trimestrale"),
    Input(
        "C_V",
        "consumi_famiglie_trimestrali",
        {
            "DATA_TYPE_AGGR": "P31_D_W0_S14",
            "EXPEND_PURPOSE_COICOPCOFOG": "CP01_13",
            "VALUATION": "V",
            "ADJUSTMENT": "N",
        },
        "flow",
        "Consumi delle famiglie residenti, prezzi correnti",
    ),
    Input(
        "C_L",
        "consumi_famiglie_trimestrali",
        {
            "DATA_TYPE_AGGR": "P31_D_W0_S14",
            "EXPEND_PURPOSE_COICOPCOFOG": "CP01_13",
            "VALUATION": "L_2020",
            "ADJUSTMENT": "N",
        },
        "flow",
        "Consumi delle famiglie residenti, valori concatenati 2020",
    ),
    Input(
        "ULA",
        "ula_trimestrali",
        {
            "DATA_TYPE_AGGR": "FT",
            "EMPLOYMENT_STATUS": "1",
            "BRKDW_INDUSTRY_NACE_REV2": "_T",
            "TYPE_EMPLOYMENT": "TOT",
            "ADJUSTMENT": "N",
        },
        "avg",
        "Unità di lavoro dipendenti, totale economia (migliaia)",
    ),
    Input(
        "POP",
        "popolazione_trimestrale",
        {"DATA_TYPE_AGGR": "POP", "ADJUSTMENT": "N"},
        "avg",
        "Popolazione media (migliaia)",
    ),
    Input(
        "WF_A",
        "ricchezza_finanziaria_trimestrale",
        {**_WEALTH, "ACCOUNTING_ENTRY": "A"},
        "stock",
        "Attività finanziarie, famiglie e ISP (fine periodo)",
    ),
    Input(
        "WF_L",
        "ricchezza_finanziaria_trimestrale",
        {**_WEALTH, "ACCOUNTING_ENTRY": "L"},
        "stock",
        "Passività finanziarie, famiglie e ISP (fine periodo)",
    ),
]

ANNUAL = [
    *_sector_account("famiglie_conto_annuale"),
    Input(
        "C_V",
        "pil_componenti_annuali",
        {"DATA_TYPE_AGGR": "P31_D_W0_S14", "VALUATION": "V", "ADJUSTMENT": "N"},
        "flow",
        "Consumi delle famiglie residenti, prezzi correnti",
    ),
    Input(
        "C_L",
        "pil_componenti_annuali",
        {"DATA_TYPE_AGGR": "P31_D_W0_S14", "VALUATION": "L_2020", "ADJUSTMENT": "N"},
        "flow",
        "Consumi delle famiglie residenti, valori concatenati 2020",
    ),
    Input(
        "ULA",
        "ula_annuali",
        {
            "DATA_TYPE_AGGR": "FT",
            "EMPLOYMENT_STATUS": "1",
            "BRKDW_INDUSTRY_NACE_REV2": "_T",
            "TYPE_EMPLOYMENT": "TOT",
            "ADJUSTMENT": "N",
        },
        "avg",
        "Unità di lavoro dipendenti, totale economia (migliaia)",
    ),
    Input(
        "POP",
        "popolazione_annuale",
        {"DATA_TYPE_AGGR": "POP", "ADJUSTMENT": "N"},
        "avg",
        "Popolazione media (migliaia)",
    ),
    # Annual wealth is the end-of-year stock, i.e. the fourth quarter.
    Input(
        "WF_A",
        "ricchezza_finanziaria_trimestrale",
        {**_WEALTH, "ACCOUNTING_ENTRY": "A"},
        "stock",
        "Attività finanziarie, famiglie e ISP (31 dicembre)",
    ),
    Input(
        "WF_L",
        "ricchezza_finanziaria_trimestrale",
        {**_WEALTH, "ACCOUNTING_ENTRY": "L"},
        "stock",
        "Passività finanziarie, famiglie e ISP (31 dicembre)",
    ),
]


@dataclass(frozen=True)
class TableSpec:
    name: str
    title: str
    inputs: list[Input]
    annual: bool

    @property
    def path(self) -> str:
        return f"{TABLE_DIR}/{self.name}.xlsx"

    @property
    def datasets(self) -> list[str]:
        return list(dict.fromkeys(i.dataset for i in self.inputs))


TABLES = [
    TableSpec(
        "tabella_reddito_trimestrale",
        "Reddito disponibile, consumi e ricchezza finanziaria delle famiglie",
        QUARTERLY,
        annual=False,
    ),
    TableSpec(
        "tabella_reddito_annuale",
        "Reddito disponibile, consumi e ricchezza finanziaria delle famiglie",
        ANNUAL,
        annual=True,
    ),
]


# ---------------------------------------------------------------------------
# Raw data
# ---------------------------------------------------------------------------


def read_inputs(spec: TableSpec, read_csv) -> pd.DataFrame:
    """Wide frame of raw values, one row per ISTAT period ('2026-Q2' or '2025')."""
    frames: dict[str, pd.DataFrame] = {}
    columns = {}
    for item in spec.inputs:
        if item.dataset not in frames:
            frames[item.dataset] = read_csv(item.dataset)
        df = frames[item.dataset]
        mask = pd.Series(True, index=df.index)
        for dim, code in item.filter.items():
            if dim in df:
                mask &= df[dim] == code
        series = df.loc[mask, ["TIME_PERIOD", "OBS_VALUE"]]
        if series.empty:
            raise ValueError(f"{spec.name}: no data for {item.col} in {item.dataset}")
        if series["TIME_PERIOD"].duplicated().any():
            raise ValueError(f"{spec.name}: filter for {item.col} matches several series")
        values = series.set_index("TIME_PERIOD")["OBS_VALUE"].astype(float)
        if spec.annual and item.kind == "stock":
            values = values[values.index.str.endswith("Q4")]
            values.index = values.index.str[:4]
        columns[item.col] = values
    wide = pd.DataFrame(columns).sort_index()
    return wide


# ---------------------------------------------------------------------------
# Workbook
# ---------------------------------------------------------------------------


def _quarter_key(period: str) -> str:
    return f"{period[:4]}-T{period[-1]}"


def _semester_key(period: str) -> str:
    return f"{period[:4]}-S{1 if int(period[-1]) <= 2 else 2}"


@dataclass
class _Layout:
    dati_cols: dict[str, str]  # input col -> letter in Dati
    serie_cols: dict[str, str]  # input col -> letter in Serie


def build_workbook(spec: TableSpec, raw: pd.DataFrame, editions: dict[str, str]) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)
    tabella = wb.create_sheet("Tabella")
    serie = wb.create_sheet("Serie")
    dati = wb.create_sheet("Dati")
    note = wb.create_sheet("Note")

    layout = _write_dati(dati, spec, raw)
    keys = _write_serie(serie, spec, raw, layout)
    _write_tabella(wb, tabella, spec, keys, layout)
    _write_note(note, spec, editions)

    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for cell in row:
                f = cell.font
                cell.font = Font(name=FONT, bold=f.b, italic=f.i, size=f.sz, color=f.color)
    wb.calculation.fullCalcOnLoad = True
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _write_dati(ws, spec: TableSpec, raw: pd.DataFrame) -> _Layout:
    if spec.annual:
        key_headers = ["ANNO"]
    else:
        key_headers = ["PERIODO_ISTAT", "TRIMESTRE", "SEMESTRE", "FINE_SEMESTRE"]
    headers = key_headers + [i.col for i in spec.inputs]
    ws.append(headers)
    ws.append([""] * len(key_headers) + [i.label for i in spec.inputs])
    for period, row in raw.iterrows():
        if spec.annual:
            keys = [str(period)]
        else:
            q = int(period[-1])
            keys = [
                period,
                _quarter_key(period),
                _semester_key(period),
                _semester_key(period) if q in (2, 4) else None,
            ]
        values = [None if pd.isna(row[i.col]) else float(row[i.col]) for i in spec.inputs]
        ws.append(keys + values)

    dati_cols = {
        i.col: get_column_letter(len(key_headers) + n + 1) for n, i in enumerate(spec.inputs)
    }
    ws.freeze_panes = ws.cell(row=3, column=len(key_headers) + 1)
    for n in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(n)].width = 14
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for cell in ws[2]:
        cell.alignment = Alignment(wrap_text=True, vertical="top")
    ws.row_dimensions[2].height = 60
    for row in ws.iter_rows(min_row=3, min_col=len(key_headers) + 1):
        for cell in row:
            cell.number_format = NUMBER
    return _Layout(dati_cols=dati_cols, serie_cols={})


def _write_serie(ws, spec: TableSpec, raw: pd.DataFrame, layout: _Layout) -> dict[str, list[str]]:
    """One row per period. Returns the period keys per frequency, newest last."""
    ws.append(["CHIAVE", "FREQUENZA"] + [i.col for i in spec.inputs])
    ws.append(["", ""] + [i.label for i in spec.inputs])
    layout.serie_cols = {i.col: get_column_letter(n + 3) for n, i in enumerate(spec.inputs)}

    periods = [str(p) for p in raw.index]
    if spec.annual:
        groups = {"Anno": [(p, "A") for p in periods]}
    else:
        semesters = list(dict.fromkeys(_semester_key(p) for p in periods))
        groups = {
            "Semestre": [(s, "S") for s in semesters],
            "Trimestre": [(_quarter_key(p), "T") for p in periods],
        }

    for rows in groups.values():
        for key, freq in rows:
            r = ws.max_row + 1
            formulas = [_aggregate(item, freq, r, layout, spec.annual) for item in spec.inputs]
            ws.append([key, freq] + formulas)

    ws.freeze_panes = "C3"
    ws.column_dimensions["A"].width = 12
    for n in range(3, len(spec.inputs) + 3):
        ws.column_dimensions[get_column_letter(n)].width = 14
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for cell in ws[2]:
        cell.alignment = Alignment(wrap_text=True, vertical="top")
    ws.row_dimensions[2].height = 60
    for row in ws.iter_rows(min_row=3, min_col=3):
        for cell in row:
            cell.number_format = NUMBER
    return {name: [k for k, _ in rows] for name, rows in groups.items()}


def _aggregate(item: Input, freq: str, row: int, layout: _Layout, annual: bool) -> str:
    col = layout.dati_cols[item.col]
    data = f"Dati!${col}:${col}"
    key = f"$A{row}"
    if annual:
        by, need = "Dati!$A:$A", 1
    elif freq == "T":
        by, need = "Dati!$B:$B", 1
    elif item.kind == "stock":
        by, need = "Dati!$D:$D", 1
    else:
        by, need = "Dati!$C:$C", 2
    count = f'COUNTIFS({by},{key},{data},"<>")'
    if item.kind == "avg" and need > 1:
        value = f"AVERAGEIFS({data},{by},{key})"
    else:
        value = f"SUMIFS({data},{by},{key})"
    # Empty text, not NA(): the sheet stays readable, and any arithmetic on it
    # in Tabella fails into "n.d." all the same.
    return f'=IF({count}<{need},"",{value})'


# Rows of the Tabella sheet: (label, expression over Serie columns, kind).
# kind: "nominal" -> /1000 to billions; "real" -> nominal deflated to the
# prices of the last selected period; "pc"/"ula" -> euro per head / per ULA.
_COMPONENTS = [
    ("Redditi da lavoro dipendente", "{D1_C}"),
    ("Risultato lordo di gestione e reddito misto", "{B2A3G}"),
    ("Redditi da capitale netti", "({D4_C}-{D4_D})"),
    ("Prestazioni sociali nette", "({D62_C}-{D62_D})"),
    ("Altri trasferimenti correnti netti", "({D7_C}-{D7_D})"),
    ("Contributi sociali netti", "({D61_C}-{D61_D})"),
    ("Imposte correnti", "({D5_C}-{D5_D})"),
    ("Rettifica per variazione dei diritti pensionistici", "({D8_C}-{D8_D})"),
]
# As in the hand-built tables: disposable income including the pension
# adjustment (B6G + D8), so that the components above add up to it.
_YD = ("Reddito disponibile lordo", "({B6G}+{D8_C}-{D8_D})")
_CONS = ("Consumi finali delle famiglie residenti", "{C_V}")
_WF = ("Ricchezza finanziaria netta (fine periodo)", "({WF_A}-{WF_L})")
_DEFL = "({C_V}/{C_L})"


def _write_tabella(wb, ws, spec: TableSpec, keys: dict[str, list[str]], layout: _Layout) -> None:
    # Lists for the drop-downs, on a hidden sheet.
    lists = wb.create_sheet("Liste")
    lists.sheet_state = "hidden"
    for n, (name, values) in enumerate(keys.items(), start=1):
        letter = get_column_letter(n)
        lists.cell(row=1, column=n, value=name)
        for r, v in enumerate(values, start=2):
            lists.cell(row=r, column=n, value=v)
        ref = f"Liste!${letter}$2:${letter}${len(values) + 1}"
        wb.defined_names[f"lista_{name}"] = DefinedName(f"lista_{name}", attr_text=ref)

    ws["A1"] = spec.title
    ws["A1"].font = Font(bold=True, size=13)
    period_cols = ["C", "D", "E", "F"]
    var_cols = ["G", "H", "I"]

    if spec.annual:
        ws["A3"] = "Anni a confronto (celle gialle):"
        choices = keys["Anno"]
        defaults = _default_years(choices)
        list_formula = "lista_Anno"
    else:
        ws["A3"] = "Frequenza:"
        ws["B3"] = "Semestre"
        ws["B3"].fill = INPUT_FILL
        dv_freq = DataValidation(type="list", formula1='"Semestre,Trimestre"', allow_blank=False)
        ws.add_data_validation(dv_freq)
        dv_freq.add("B3")
        ws["C3"] = "Scegli i quattro periodi nelle celle gialle; l'ultimo è il riferimento."
        ws["C3"].font = Font(italic=True)
        defaults = _default_semesters(keys["Semestre"])
        list_formula = 'INDIRECT(IF($B$3="Semestre","lista_Semestre","lista_Trimestre"))'

    ws["A5"] = "Voce"
    ws["B5"] = "Unità"
    for col, value in zip(period_cols, defaults, strict=True):
        ws[f"{col}5"] = value
        ws[f"{col}5"].fill = INPUT_FILL
    dv = DataValidation(type="list", formula1=list_formula, allow_blank=False)
    ws.add_data_validation(dv)
    dv.add("C5:F5")
    for col, base in zip(var_cols, period_cols[:3], strict=True):
        ws[f"{col}5"] = f'="Var. % "&$F$5&" su "&{base}$5'
    for cell in ws[5]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(wrap_text=True, horizontal="center", vertical="center")
    ws.row_dimensions[5].height = 32

    def lookup(col: str, period_cell: str) -> str:
        letter = layout.serie_cols[col]
        return f"INDEX(Serie!${letter}:${letter},MATCH({period_cell},Serie!$A:$A,0))"

    def expr(template: str, period_cell: str) -> str:
        return template.format(**{c: lookup(c, period_cell) for c in layout.serie_cols})

    row = 6

    def section(title: str) -> None:
        nonlocal row
        row += 1
        ws.cell(row=row, column=1, value=title).font = Font(bold=True)
        for c in range(1, 10):
            ws.cell(row=row, column=c).fill = HEAD_FILL
        row += 1

    def line(
        label: str, unit: str, make, *, bold=False, italic=False, top=False, changes=True
    ) -> None:
        nonlocal row
        ws.cell(row=row, column=1, value=label)
        ws.cell(row=row, column=2, value=unit)
        for col in period_cols:
            ws[f"{col}{row}"] = f'=IFERROR({make(f"{col}$5")},"n.d.")'
        for col, base in zip(var_cols if changes else [], period_cols[:3], strict=False):
            last, ref = f"$F{row}", f"{base}{row}"
            ws[f"{col}{row}"] = (
                f'=IFERROR(IF(SIGN({last})*SIGN({ref})<0,"n.s.",({last}/{ref}-1)*100),"n.d.")'
            )
        for c in range(1, 10):
            cell = ws.cell(row=row, column=c)
            cell.font = Font(bold=bold, italic=italic)
            if c >= 3:
                cell.number_format = NUMBER
                cell.alignment = Alignment(horizontal="right")
            if top:
                cell.border = Border(top=Side(style="medium"))
        row += 1

    nominal = lambda t: lambda p: f"{expr(t, p)}/1000"  # noqa: E731
    real = lambda t: lambda p: f"{expr(t, p)}/{expr(_DEFL, p)}*{expr(_DEFL, '$F$5')}/1000"  # noqa: E731

    section("Prezzi correnti")
    for label, template in _COMPONENTS:
        line(label, "mld €", nominal(template))
    line(_YD[0], "mld €", nominal(_YD[1]), bold=True, top=True)
    line(_CONS[0], "mld €", nominal(_CONS[1]))
    line(_WF[0], "mld €", nominal(_WF[1]))

    ws.cell(row=row + 1, column=1, value="")
    row += 1
    ws.cell(row=row, column=1).value = '="A prezzi del periodo "&$F$5&" (deflatore dei consumi)"'
    ws.cell(row=row, column=1).font = Font(bold=True)
    for c in range(1, 10):
        ws.cell(row=row, column=c).fill = HEAD_FILL
    row += 1
    for label, template in _COMPONENTS:
        line(label, "mld €", real(template))
    line(_YD[0], "mld €", real(_YD[1]), bold=True, top=True)
    line(_CONS[0], "mld €", real(_CONS[1]))
    line(_WF[0], "mld €", real(_WF[1]))

    per = "nell'anno" if spec.annual else "nel periodo"
    section(f"Per abitante e per unità di lavoro, a prezzi dell'ultimo periodo ({per})")
    defl = lambda p: f"/{expr(_DEFL, p)}*{expr(_DEFL, '$F$5')}"  # noqa: E731
    line("Popolazione media", "milioni", lambda p: f"{expr('{POP}', p)}/1000")
    line("ULA dipendenti", "milioni", lambda p: f"{expr('{ULA}', p)}/1000")
    line(
        "Reddito disponibile lordo per abitante",
        "€",
        lambda p: f"{expr(_YD[1], p)}*1000/{expr('{POP}', p)}{defl(p)}",
        italic=True,
    )
    line(
        "Consumi per abitante",
        "€",
        lambda p: f"{expr('{C_V}', p)}*1000/{expr('{POP}', p)}{defl(p)}",
        italic=True,
    )
    line(
        "Redditi da lavoro dipendente per ULA dipendente",
        "€",
        lambda p: f"{expr('{D1_C}', p)}*1000/{expr('{ULA}', p)}{defl(p)}",
        italic=True,
    )
    line(
        "Ricchezza finanziaria netta per abitante",
        "migliaia €",
        lambda p: f"{expr('({WF_A}-{WF_L})', p)}/{expr('{POP}', p)}{defl(p)}",
        italic=True,
    )

    section("Indicatori")
    line(
        "Deflatore dei consumi (ultimo periodo = 100)",
        "indice",
        lambda p: f"{expr(_DEFL, p)}/{expr(_DEFL, '$F$5')}*100",
    )
    line(
        "Propensione al consumo",
        "% del reddito",
        lambda p: f"{expr('{C_V}', p)}/{expr(_YD[1], p)}*100",
    )
    line(
        "Reddito disponibile lordo senza rettifica D8 (B6G), prezzi correnti",
        "mld €",
        lambda p: f"{expr('{B6G}', p)}/1000",
    )
    line(
        "Controllo: componenti meno reddito disponibile",
        "mln €",
        lambda p: expr(
            "{B2A3G}+{D1_C}+{D4_C}-{D4_D}+{D5_C}-{D5_D}+{D61_C}-{D61_D}"
            "+{D62_C}-{D62_D}+{D7_C}-{D7_D}-{B6G}",
            p,
        ),
        italic=True,
        changes=False,
    )

    row += 1
    notes = [
        "Dati grezzi (non destagionalizzati), ultima edizione ISTAT: vedi il foglio Note.",
        "Flussi sommati sui trimestri del periodo; ULA e popolazione: media dei trimestri; "
        "ricchezza: consistenza a fine periodo.",
        "n.d. = dato non disponibile o periodo incompleto; n.s. = variazione non significativa "
        "(cambio di segno).",
    ]
    for text in notes:
        ws.cell(row=row, column=1, value=text).font = Font(italic=True, size=9)
        row += 1

    red = Font(color="C00000")
    ws.conditional_formatting.add(
        f"C7:I{row}", CellIsRule(operator="lessThan", formula=["0"], font=red)
    )
    ws.column_dimensions["A"].width = 52
    ws.column_dimensions["B"].width = 12
    for col in period_cols + var_cols:
        ws.column_dimensions[col].width = 14
    ws.freeze_panes = "C6"


def _default_semesters(semesters: list[str]) -> list[str]:
    """The layout of the 2025-2026 tables: same half-year of 2007, 2019,
    previous year and latest year. Falls back to the latest four."""
    last = semesters[-1]
    year, half = int(last[:4]), last[-2:]
    wanted = [f"{y}-{half}" for y in (2007, 2019, year - 1, year)]
    if all(w in semesters for w in wanted) and len(set(wanted)) == 4:
        return wanted
    return (semesters[-4:] + [last] * 4)[:4]


def _default_years(years: list[str]) -> list[str]:
    last = int(years[-1])
    wanted = [str(y) for y in (2007, 2019, last - 1, last)]
    if all(w in years for w in wanted) and len(set(wanted)) == 4:
        return wanted
    return (years[-4:] + [years[-1]] * 4)[:4]


def _write_note(ws, spec: TableSpec, editions: dict[str, str]) -> None:
    rows = [
        ("Generato il", datetime.now().strftime("%Y-%m-%d %H:%M")),
        ("", ""),
        ("Dataset", "Edizione / ultimo dato"),
        *[(ds, editions.get(ds, "-")) for ds in spec.datasets],
        ("", ""),
        ("Definizioni", ""),
        (
            "Reddito disponibile",
            "Famiglie consumatrici (S14A): B6G più la rettifica per la variazione dei diritti "
            "pensionistici (D8), come nelle tabelle precedenti. Lordo di ammortamenti, già al "
            "netto di imposte e contributi. B6G senza rettifica è tra gli indicatori.",
        ),
        (
            "Componenti",
            "Ogni voce è risorse meno impieghi; la riga di controllo verifica che la somma "
            "torni al reddito disponibile (differenze di arrotondamento di qualche decimo).",
        ),
        (
            "Consumi",
            "Spesa per consumi finali delle famiglie residenti, sul territorio e all'estero "
            "(P31_D_W0_S14).",
        ),
        (
            "Deflatore",
            "Somma dei consumi a prezzi correnti / somma dei valori concatenati sul periodo "
            "(non media dei deflatori trimestrali). I valori reali sono ai prezzi "
            "dell'ultimo periodo scelto.",
        ),
        (
            "Ricchezza finanziaria",
            "Banca d'Italia, conti finanziari (TFAT0012), via BCE (QSA): attività meno "
            "passività di famiglie e ISP (S1M), perimetro diverso da S14A.",
        ),
        (
            "Ricchezza immobiliare",
            "Non inclusa: esce solo nel rapporto annuale ISTAT-Banca d'Italia e va stimata.",
        ),
        (
            "Per abitante / per ULA",
            "Valori del periodo, non annualizzati. Popolazione media di contabilità nazionale; "
            "ULA dei dipendenti, totale economia.",
        ),
    ]
    for r in rows:
        ws.append(list(r))
    ws.column_dimensions["A"].width = 34
    ws.column_dimensions["B"].width = 110
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")
    ws["A3"].font = Font(bold=True)
    ws["A6"].font = Font(bold=True)


# ---------------------------------------------------------------------------
# Rebuild on change
# ---------------------------------------------------------------------------


def build_tables(sink, manifest: dict, force: bool = False) -> list[str]:
    """Rebuild the tables whose inputs changed since the last build.

    Returns the paths written. The input fingerprints live in the manifest
    under "_tabelle", next to the datasets.
    """
    state = manifest.setdefault("_tabelle", {})
    written = []
    for spec in TABLES:
        missing = [d for d in spec.datasets if d not in manifest]
        if missing:
            log.info("[%s] skipped: %s not published yet", spec.name, ", ".join(missing))
            continue
        inputs = {d: manifest[d]["sha256"] for d in spec.datasets}
        if not force and state.get(spec.name, {}).get("inputs") == inputs:
            continue

        def read_csv(dataset: str) -> pd.DataFrame:
            path = next(p for p in manifest[dataset]["files"] if p.endswith(".csv"))
            data = sink.read(path)
            if data is None:
                raise FileNotFoundError(path)
            return pd.read_csv(io.BytesIO(data), encoding="utf-8-sig", dtype=str).assign(
                OBS_VALUE=lambda frame: pd.to_numeric(frame["OBS_VALUE"], errors="coerce")
            )

        raw = read_inputs(spec, read_csv)
        editions = {
            d: manifest[d].get("edition") or f"dati fino a {manifest[d].get('last_period')}"
            for d in spec.datasets
        }
        previous = state.get(spec.name)
        if previous:
            stamp = previous.get("built_at", "")[:10].replace("-", "")
            sink.copy(spec.path, f"archivio/{TABLE_DIR}/{spec.name}_{stamp}.xlsx")
        sink.write(spec.path, build_workbook(spec, raw, editions))
        state[spec.name] = {
            "inputs": inputs,
            "built_at": datetime.now().isoformat(timespec="seconds"),
            "periods": f"{raw.index.min()} – {raw.index.max()}",
        }
        log.info("[%s] written (%s)", spec.name, json.dumps(editions, ensure_ascii=False))
        written.append(spec.path)
    return written
