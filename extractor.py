#!/usr/bin/env python3
"""
Single‑document extractor for NY‑UAS Comprehensive Community Health Assessment
-----------------------------------------------------------------------------

• Reads a .docx report generated from your template
• Pulls all the labels listed in LABELS
• Writes ONE spreadsheet:
      Row 1 = label headers
      Row 2 = extracted values (blank if not found)

Dependencies: python‑docx, pandas, PyYAML (no NLTK).

Usage
-----
python extractor.py note.docx                # → output.xlsx
python extractor.py note.docx -o out.csv     # → out.csv
"""

import re, argparse, yaml, pandas as pd
from pathlib import Path
from docx import Document


# ──────────────────────────── label list (headers) ───────────────────────────
LABELS = """
last first dob cin asm_date a_present a_source a_mode caregiver_assist a_goc a_omcg a_cgcomm a_lvstatus a_lvarr a_ed a_sect_comments
b_shortmem b_procmem b_sect_comments c_sect_comments d_pleasure d_anxious d_sad d_sect_comments
e_social e_family e_other e_alone e_stress e_sect_comments
f_mealperf f_mealcap f_hswperf f_hswcap f_fncperf f_fnccap f_medperf f_medcap f_phnperf f_phncap f_stairperf f_staircap
f_shopperf f_shopcap f_transperf f_transcap f_bathing f_hygiene f_dressup f_dresslow f_walk f_loco f_transtoilet f_toiletuse
f_bedmob f_eating f_mode f_exercise f_out f_adlchange f_suffchange f_drove f_stopdrv f_toltrans f_sect_comments
g_bladder g_bowel g_sect_comments
h_hip h_other h_alz h_demen h_stroke h_chd h_copd h_chf h_anx h_bpd h_depr h_schiz h_covid h_cancer h_dm h_sect_comments
i_falls i_noinj i_mininj i_majinj i_dizzi i_gait i_chest i_atp i_ffb i_hallu i_reflux i_const i_diarr i_vomit i_nonsleep i_toosleep
i_dyspnea i_fat i_painfreq i_painint i_paincons i_painbrkt i_paincntrl i_cond i_exp i_health i_smoke i_chew i_drinks i_drinkcut
i_drinkcrit i_drinkguilt i_drinkeye i_drinksoc i_sect_comments
j_weight j_dehyd j_fluidin j_fluidout j_mode j_sect_comments
k_rx k_allergy k_allcat k_allother k_sect_comments
l_bp l_colon l_dental l_eye l_hearing l_influ l_mammo l_pneu l_covid l_inpatient l_er l_phys l_facility l_impmed l_inj l_resp l_wound
l_hhdiab l_gibleed l_heart l_mcis l_chemo l_surg l_uti l_iv l_dvtpe l_pain l_psycho l_other l_unknown l_impmeder l_nausea l_injer
l_resper l_wounder l_cardiac l_hhdiaber l_gibleeder l_otherer l_unknowner l_therapy l_respite l_eolc l_perm l_unsafe l_othernh
l_unknh l_sect_comments
m_family m_commun m_sect_comments
n_food n_shelter n_clothing n_meds n_hvac n_health n_sect_comments
""".split()

# add medication labels programmatically (ma_drug1 … ma_notes26)
for i in range(1, 27):
    LABELS.extend([
        f"ma_drug{i}", f"mad{i}", f"ma_unit{i}", f"ma_route{i}", f"ma_frq{i}",
        f"p{i}", f"ma_notes{i}", f"notes{i}"
    ])

# remaining clinical flags
LABELS.extend("""
chad_bp chad_copd chad_dm chad_heart chad_hip chad_odem chad_ofrac
fsd_hemi fsd_ms fsd_para fsd_park fsd_pneu
od_d1 od_dd1 od_icd1 od_d2 od_dd2 od_icd2 od_d3 od_dd3 od_icd3 od_d4 od_dd4 od_icd4
sec_age sec_loc sec_120 sec_adl1 sec_adl2 sec_adl3 sf_120 sf_sched sf_alone
""".split())
# ──────────────────────────────────────────────────────────────────────────────


# ───────────────────────────── helper functions ───────────────────────────────
def read_docx(path: Path) -> str:
    """Return full text of the .docx (one line per paragraph)."""
    return "\n".join(p.text for p in Document(path).paragraphs)

def first_n_sentences(text: str, n=2) -> str:
    """
    *Lightweight* sentence splitter (no NLTK).
    Splits on ., !, ? followed by whitespace and an uppercase letter.
    Good enough for short clinical paragraphs.
    """
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text.strip())
    return " ".join(sentences[:n])

def sectionize(text: str) -> dict:
    """
    Slice the document into {header -> body}. Header line must end with ':'
    and be either ALL‑CAPS or Title‑Case (e.g., 'SECTION A:' or 'Chief Complaint:').
    """
    sections, current = {}, "_preamble"
    for line in text.splitlines():
        m = re.match(r"^([\w ,/()]+):\s*$", line)
        if m and (line.isupper() or line.istitle()):
            current = m.group(1).lower()
            sections[current] = []
        else:
            sections.setdefault(current, []).append(line)
    return {k: "\n".join(v).strip() for k, v in sections.items()}

def postprocess(label, value):
    """Tidy header 'LAST, FIRST' → split into separate fields."""
    if label in {"last", "first"} and "," in value:
        last, first = [x.strip() for x in value.split(",", 1)]
        return last if label == "last" else first
    return value.strip("  ")   # remove any non‑breaking spaces


# ───────────────────── YAML rules loader & wildcard expander ──────────────────
def load_rules() -> dict:
    return yaml.safe_load(Path("label_map.yml").read_text())

def expand_wildcards(rules: dict, max_n: int = 30) -> dict:
    out = {}
    for label, rule in rules.items():
        if "*" in label:
            for i in range(1, max_n + 1):
                out[label.replace("*", str(i))] = {**rule, "row": i - 1}
        else:
            out[label] = rule
    return out


# ───────────────────────────── main extraction ────────────────────────────────
def extract(path: Path) -> dict:
    text = read_docx(path)
    sections = sectionize(text)
    rules = expand_wildcards(load_rules())

    row = {lab: "" for lab in LABELS}

    for label, rule in rules.items():
        variants = rule["search"]
        # Find sections whose name contains any variant
        candidate_secs = [
            s for name, s in sections.items()
            if any(re.search(v, name, re.I) for v in variants)
        ] or [text]

        if rule["type"] == "single_line":
            value = ""
            for sec in candidate_secs:
                for v in variants:
                    pattern = rf"{re.escape(v)}[:\s]*([^\n]{{1,200}}?)(?:\s\s|\n|$)"
                    if (m := re.search(pattern, sec, flags=re.I)):
                        value = m.group(1).strip()
                        break
                if value:
                    break

        elif rule["type"] == "paragraph":
            value = first_n_sentences(candidate_secs[0],
                                      rule.get("keep_n_sentences", 2))
        else:
            value = ""

        row[label] = postprocess(label, value)

    return row


# ───────────────────────── CSV / Excel writer ────────────────────────────────
def write_row(row, headers, out_path):
    ordered = [row.get(h, "") for h in headers]
    df = pd.DataFrame([ordered], columns=headers)
    (df.to_csv if out_path.lower().endswith(".csv") else df.to_excel)(out_path,
                                                                     index=False)


# ───────────────────────────── CLI entrypoint ────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Extract NY‑UAS .docx → spreadsheet")
    ap.add_argument("docx", help="Path to .docx file")
    ap.add_argument("-o", "--out", default="output.xlsx",
                    help="Output file (.xlsx | .csv)")
    args = ap.parse_args()

    data = extract(Path(args.docx))
    write_row(data, LABELS, args.out)
    print(f"✅  Saved {args.out}")
